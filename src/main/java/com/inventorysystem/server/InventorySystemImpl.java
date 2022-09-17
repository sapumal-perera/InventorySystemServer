package com.inventorysystem.server;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.inventorymanager.communication.grpc.generated.*;
import com.inventorysystem.sycnhronization.DistributedTxCoordinator;
import com.inventorysystem.sycnhronization.DistributedTxListener;
import com.inventorysystem.sycnhronization.DistributedTxParticipant;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

public class InventorySystemImpl extends InventoryServiceGrpc.InventoryServiceImplBase implements DistributedTxListener {

    private Pair<String, InventoryOperationRequest> tempDataHolder;
    private ManagedChannel channel = null;
    private InventoryServiceGrpc.InventoryServiceBlockingStub clientStub = null;
    private InventorySystemServer server;
    private String transactionStatus = "";

    public InventorySystemImpl(InventorySystemServer server) {
        this.server = server;
    }

    @Override
    public void onGlobalCommit() {
        performClientRequest();
    }

    @Override
    public void onGlobalAbort() {
        tempDataHolder = null;
        System.out.println("Transaction is aborted by the Coordinator");
    }

    private void startDistributedTx(String itemCode, InventoryOperationRequest request) {
        try {
            server.getOrderTransaction().start(itemCode, String.valueOf(UUID.randomUUID()));
            tempDataHolder = new Pair<>(itemCode, request);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private InventoryOperationResponse callServer(String itemCode, InventoryOperationRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println("Call Server " + IPAddress + ":" + port);
        channel = ManagedChannelBuilder.forAddress(IPAddress, port)
                .usePlaintext()
                .build();
        clientStub = InventoryServiceGrpc.newBlockingStub(channel);

        InventoryOperationRequest orderRequest = InventoryOperationRequest.newBuilder(request)
                .setIsSentByPrimary(isSentByPrimary)
                .build();
        if (orderRequest.getOperationType().equals(OperationTypes.ORDER.name())) {
            return clientStub.orderItem(orderRequest);
        } else {
            return clientStub.updateInventory(orderRequest);
        }
    }

    private InventoryOperationResponse callPrimary(String itemCode, InventoryOperationRequest request) {
        System.out.println("Calling Primary server");
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt(currentLeaderData[1]);
        return callServer(itemCode, request, false, IPAddress, port);
    }

    private void updateSecondaryServers(String itemCode, InventoryOperationRequest request) throws KeeperException, InterruptedException {
        System.out.println("Updating secondary servers");
        List<String[]> othersData = server.getOthersData();
        for (String[] data : othersData) {
            String IPAddress = data[0];
            int port = Integer.parseInt(data[1]);
            callServer(itemCode, request, true, IPAddress, port);
        }
    }

    public void orderItem(InventoryOperationRequest request, StreamObserver<InventoryOperationResponse> responseObserver) {
        String itemCode = request.getItemCode();

        if (server.isLeader()) {
            try {
                System.out.println("Processing received client request as Primary");
                startDistributedTx(itemCode, request);
                updateSecondaryServers(itemCode, request);
                if (server.getInventoryDb().containsKey(itemCode) && (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) > 0 || (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) == 0) {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).perform();
                    transactionStatus = "Request " + request.getRequsetId() + " for " + request.getItemCode() + " Successfully processed";
                } else if (server.getInventoryDb().containsKey(itemCode) && (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) < 0) {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).perform();
                    transactionStatus = "To perform Request " + request.getRequsetId() + " for " + request.getItemCode() + " added to the queue since insufficient item quantity. Request will be execute later.";
                } else {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).sendGlobalAbort();
                    transactionStatus = "Request failed. Please try again later";
                }
            } catch (Exception e) {
                System.out.println("Error while processing the client request" + e.getMessage());
                e.printStackTrace();
                transactionStatus = "Request failed. Please try again later";
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing received client request as Secondary, on Primary's command");
                startDistributedTx(itemCode, request);
                if (server.getInventoryDb().containsKey(itemCode) && (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) > 0 || (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) == 0) {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteCommit();
                    transactionStatus = "Request " + request.getRequsetId() + " for " + request.getItemCode() + " Successfully processed";
                } else if (server.getInventoryDb().containsKey(itemCode) && (server.getInventoryDb().get(itemCode).getQuantity() - request.getQuantity()) < 0) {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteCommit();
                    transactionStatus = "To perform Request " + request.getRequsetId() + " for " + request.getItemCode() + " added to the queue since insufficient item quantity. Request will be execute later.";
                } else {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteAbort();
                    transactionStatus = "Error occurred";
                }
            } else {
                InventoryOperationResponse response = callPrimary(itemCode, request);
                if (response.getStatus().equals("SUCCESSFUL")) {
                    transactionStatus = "Request processing successfully.";
                }
            }
        }
        InventoryOperationResponse response = InventoryOperationResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    public void updateInventory(InventoryOperationRequest request, StreamObserver<InventoryOperationResponse> responseObserver) {
        String itemCode = request.getItemCode();
        if (server.isLeader()) {
            try {
                System.out.println("Processing received client request as Primary");
                startDistributedTx(itemCode, request);
                updateSecondaryServers(itemCode, request);
                if (server.getInventoryDb().containsKey(itemCode)) {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).perform();
                    if (server.getOrderRequestList().size() == 0) {
                        transactionStatus = "Request " + request.getRequsetId() + " for updating" + request.getItemCode() + " Successfully processed";
                    } else {
                        transactionStatus = "Updating" + request.getItemCode() + " Successfully processed. Pending order requests will be executed.";
                    }
                } else {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).sendGlobalAbort();
                    transactionStatus = "This item is not exist. Please check the code and try again";
                }
            } catch (Exception e) {
                System.out.println("Error while processing the client request" + e.getMessage());
                e.printStackTrace();
                transactionStatus = "Request failed. Please try again later";
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing received client request as Secondary, on Primary's command");
                startDistributedTx(itemCode, request);
                if (server.getInventoryDb().containsKey(itemCode)) {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteCommit();
                    transactionStatus = "Request " + request.getRequsetId() + " for updating" + request.getItemCode() + " Successfully processed";
                } else {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteAbort();
                    transactionStatus = "Request failed. Please try again later";
                }
            } else {
                InventoryOperationResponse response = callPrimary(itemCode, request);
                if (response.getStatus().equals("SUCCESSFUL")) {
                    transactionStatus = "Request processing successfully.";
                }
            }
        }
        InventoryOperationResponse response = InventoryOperationResponse
                .newBuilder()
                .setStatus(transactionStatus)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    private void performClientRequest() {
        List<InventoryOperationRequest> orderRequestList = server.getOrderRequestList();
        InventoryOperationRequest orderRequest = null;
        if (tempDataHolder != null) {
            String itemCode = tempDataHolder.getKey();
            InventoryOperationRequest request = tempDataHolder.getValue();
            String operationType = request.getOperationType();
            Map<String, ItemDetail> items = server.getInventoryDb();
            if (items.containsKey(itemCode)) {
                ItemDetail data = items.get(itemCode);
                if (operationType.equals(OperationTypes.ORDER.name())) {
                    System.out.println(" Order Hit. data.getQuantity()" + data.getQuantity());
                    System.out.println(" Order Hit. request.getQuantity()" + request.getQuantity());
                    if ((data.getQuantity() - request.getQuantity()) > 0 || (data.getQuantity() - request.getQuantity()) == 0) {
                        System.out.println(" Order if. request.getQuantity()" + request.getQuantity());
                        server.getInventoryDb().get(itemCode).setQuantity(data.getQuantity() - request.getQuantity());
                    } else {
                        System.out.println(" Order Queue. request.getQuantity()" + request.getQuantity());
                        orderRequestList.add(request);
                        System.out.println(itemCode + " Order added to the queue");
                    }
                    System.out.println(itemCode + " order is successful");
                } else if (operationType.equals(OperationTypes.UPDATE.name())) {
                    System.out.println(" Order Update. request.getQuantity()" + request.getQuantity());
                    server.getInventoryDb().get(itemCode).setQuantity(data.getQuantity() + request.getQuantity());
                    for (InventoryOperationRequest orderReq : orderRequestList) {
                        if (orderReq.getItemCode().equals(request.getItemCode()) && (orderReq.getQuantity() < server.getInventoryDb().get(itemCode).getQuantity())) {
                            System.out.println("Item code "+ orderReq.getItemCode() + " quantuty " +  orderReq.getQuantity());
                            System.out.println(" Order Queue  Run. request.getItemCode()" + request.getItemCode());

                            server.getInventoryDb().get(orderReq.getItemCode()).setQuantity(server.getInventoryDb().get(itemCode).getQuantity() - orderReq.getQuantity());
                            orderRequestList.remove(orderReq);
                            System.out.println(orderReq.getItemCode() + " order executed successfully");

                        }
                    }
                    System.out.println(itemCode + " added to the inventory");
                }
            }
            tempDataHolder = null;
        }
    }

    public void getInventory(Empty request, StreamObserver<Item> responseObserver) {
        this.server.getInventoryDb()
                .entrySet()
                .stream()
                .map(entry -> Item.newBuilder()
                        .setItemCode(entry.getKey())
                        .setName(entry.getValue().getName())
                        .setQuantity(entry.getValue().getQuantity())
                        .setPrice(entry.getValue().getPrice())
                        .build())
                .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }
}

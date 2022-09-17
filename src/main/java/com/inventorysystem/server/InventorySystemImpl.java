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
        return clientStub.orderItem(orderRequest);
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
                if (server.getInventoryDb().containsKey(itemCode)) {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).perform();
                    transactionStatus = "SUCCESSFUL";
                } else {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).sendGlobalAbort();
                    transactionStatus = "FAILED";
                }
            } catch (Exception e) {
                System.out.println("Error while processing the client request" + e.getMessage());
                e.printStackTrace();
                transactionStatus = "FAILED";
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing received client request as Secondary, on Primary's command");
                startDistributedTx(itemCode, request);

                if (server.getInventoryDb().containsKey(itemCode)) {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteCommit();
                    transactionStatus = "SUCCESSFUL";
                } else {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteAbort();
                    transactionStatus = "FAILED";
                }
            } else {
                InventoryOperationResponse response = callPrimary(itemCode, request);
                if (response.getStatus().equals("SUCCESSFUL")) {
                    transactionStatus = "SUCCESSFUL";
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
                    transactionStatus = "SUCCESSFUL";
                } else {
                    ((DistributedTxCoordinator) server.getOrderTransaction()).sendGlobalAbort();
                    transactionStatus = "FAILED";
                }
            } catch (Exception e) {
                System.out.println("Error while processing the client request" + e.getMessage());
                e.printStackTrace();
                transactionStatus = "FAILED";
            }
        } else {
            if (request.getIsSentByPrimary()) {
                System.out.println("Processing received client request as Secondary, on Primary's command");
                startDistributedTx(itemCode, request);
                if (server.getInventoryDb().containsKey(itemCode)) {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteCommit();
                    transactionStatus = "SUCCESSFUL";
                } else {
                    ((DistributedTxParticipant) server.getOrderTransaction()).voteAbort();
                    transactionStatus = "FAILED";
                }
            } else {
                InventoryOperationResponse response = callPrimary(itemCode, request);
                if (response.getStatus().equals("SUCCESSFUL")) {
                    transactionStatus = "SUCCESSFUL";
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
                    if ((data.getQuantity() -request.getQuantity())> 0 || (data.getQuantity() -request.getQuantity()) == 0) {
                        System.out.println(" Order if. request.getQuantity()" + request.getQuantity());
                        server.getInventoryDb().get(itemCode).setQuantity(data.getQuantity() -request.getQuantity());
                    } else {
                        System.out.println(" Order Queue. request.getQuantity()" + request.getQuantity());
                        orderRequestList.add(request);
                        System.out.println(itemCode + " Order added to the queue");
                    }
                    System.out.println(itemCode + " order is successful");
                } else if (operationType.equals(OperationTypes.UPDATE.name())) {
                    System.out.println(" Order Update. request.getQuantity()" + request.getQuantity());

                    server.getInventoryDb().get(itemCode).setQuantity(data.getQuantity() + request.getQuantity());
                    for ( InventoryOperationRequest orderReq:  orderRequestList) {
                        if(orderReq.getItemCode().equals(request.getItemCode()) && (orderReq.getQuantity() < server.getInventoryDb().get(itemCode).getQuantity())) {
                            System.out.println(" Order Queue  Run. request.getItemCode()" + request.getItemCode());

                            server.getInventoryDb().get(orderReq.getItemCode()).setQuantity(server.getInventoryDb().get(itemCode).getQuantity() - orderReq.getQuantity());
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

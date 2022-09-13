package com.inventorysystem.server;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import com.stock.trade.sycnhronization.DistributedTxCoordinator;
import com.stock.trade.sycnhronization.DistributedTxListener;
import com.stock.trade.sycnhronization.DistributedTxParticipant;
import com.trade.communication.grpc.generated.Empty;
import com.trade.communication.grpc.generated.OrderRequest;
import com.trade.communication.grpc.generated.OrderResponse;
import com.trade.communication.grpc.generated.StockMarketRegisteredRecord;
import com.trade.communication.grpc.generated.StockTradingServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import javafx.util.Pair;
import org.apache.zookeeper.KeeperException;

public class InventorySystemImpl extends StockTradingServiceGrpc.StockTradingServiceImplBase implements DistributedTxListener {

    private Pair<String, OrderRequest> tempDataHolder;
    private ManagedChannel channel = null;
    private StockTradingServiceGrpc.StockTradingServiceBlockingStub clientStub = null;
    private InventorySystemServer server;

    //transaction status
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
        System.out.println( "Trading is aborted by the Coordinator" );
    }

    private void startDistributedTx(String stockSymbol, OrderRequest request) {
        try {
            server.getStockTransaction().start( stockSymbol, String.valueOf( UUID.randomUUID() ) );
            tempDataHolder = new Pair<>( stockSymbol, request );
        }
        catch ( IOException e ) {
            e.printStackTrace();
        }
    }

    private OrderResponse callServer(String stockSymbol, OrderRequest request, boolean isSentByPrimary, String IPAddress, int port) {
        System.out.println( "Call Server " + IPAddress + ":" + port );
        channel = ManagedChannelBuilder.forAddress( IPAddress, port )
                .usePlaintext()
                .build();
        clientStub = StockTradingServiceGrpc.newBlockingStub( channel );

        OrderRequest orderRequest = OrderRequest.newBuilder( request )
                .setIsSentByPrimary( isSentByPrimary )
                .build();
        return clientStub.tradeOperation( orderRequest );
    }

    private OrderResponse callPrimary(String stockSymbol, OrderRequest request) {
        System.out.println( "Calling Primary server" );
        String[] currentLeaderData = server.getCurrentLeaderData();
        String IPAddress = currentLeaderData[0];
        int port = Integer.parseInt( currentLeaderData[1] );
        return callServer( stockSymbol, request, false, IPAddress, port );
    }

    private void updateSecondaryServers(String stockSymbol, OrderRequest request) throws KeeperException, InterruptedException {
        System.out.println( "Updating secondary servers" );
        List<String[]> othersData = server.getOthersData();
        for ( String[] data : othersData ) {
            String IPAddress = data[0];
            int port = Integer.parseInt( data[1] );
            callServer( stockSymbol, request, true, IPAddress, port );
        }
    }

    //display currently registered stocks
    @Override
    public void getStockMarketRegisteredData(Empty request, StreamObserver<StockMarketRegisteredRecord> responseObserver) {
        this.server.getRegisteredStockDb()
                .entrySet()
                .stream()
                .map( entry -> StockMarketRegisteredRecord.newBuilder()
                        .setName( entry.getKey() )
                        .setQuantity( entry.getValue() )
                        .build() )
                .forEach( responseObserver::onNext );
        responseObserver.onCompleted();
    }

    //perform client request, primary server and secondary servers
    @Override
    public void tradeOperation(OrderRequest request, StreamObserver<OrderResponse> responseObserver) {
        String stockSymbol = request.getStockSymbol();

        if ( server.isLeader() ) {
            // Act as primary
            try {
                System.out.println( "Processing received client request as Primary" );
                startDistributedTx( stockSymbol, request );
                updateSecondaryServers( stockSymbol, request );
                //BUY OR SELL
                if(request.getOrderType() != "DELETE"){
                    if ( server.getRegisteredStockDb().get( stockSymbol.toUpperCase( Locale.ENGLISH ) ) >= request.getQuantity() ) {
                        ( (DistributedTxCoordinator) server.getStockTransaction() ).perform();
                        transactionStatus = "SUCCESSFUL";
                    }
                    else {
                        ( (DistributedTxCoordinator) server.getStockTransaction() ).sendGlobalAbort();
                        transactionStatus = "FAILED";
                    }
                }
                //DELETE
                else{
                    List<OrderRequest> orderBook = server.getOrderBook();
                    for ( OrderRequest orderRequest : orderBook ) {
                        if ( orderRequest.getId() == request.getId() ) {
                            ( (DistributedTxCoordinator) server.getStockTransaction() ).perform();
                            transactionStatus = "SUCCESSFUL";
                        }
                        else {
                            ( (DistributedTxCoordinator) server.getStockTransaction() ).sendGlobalAbort();
                            transactionStatus = "FAILED";
                        }
                    }
                }

            }
            catch ( Exception e ) {
                System.out.println( "Error while processing the client request" + e.getMessage() );
                e.printStackTrace();
                transactionStatus = "FAILED";
            }
        }
        else {
            // Act As Secondary
            if ( request.getIsSentByPrimary() ) {
                System.out.println( "Processing received client request as Secondary, on Primary's command" );
                startDistributedTx( stockSymbol, request );

                //BUY OR SELL
                if(request.getOrderType() != "DELETE") {
                    if (server.getRegisteredStockDb().get(stockSymbol.toUpperCase(Locale.ENGLISH)) >= request.getQuantity()) {
                        ((DistributedTxParticipant) server.getStockTransaction()).voteCommit();
                        transactionStatus = "SUCCESSFUL";
                    } else {
                        ((DistributedTxParticipant) server.getStockTransaction()).voteAbort();
                        transactionStatus = "FAILED";
                    }
                }

                //DELETE
                else{
                    List<OrderRequest> orderBook = server.getOrderBook();
                    for ( OrderRequest orderRequest : orderBook ) {
                        if ( orderRequest.getId() == request.getId() ) {
                            ((DistributedTxParticipant) server.getStockTransaction()).voteCommit();
                            transactionStatus = "SUCCESSFUL";
                        }
                        else {
                            ((DistributedTxParticipant) server.getStockTransaction()).voteAbort();
                            transactionStatus = "FAILED";
                        }
                    }
                }
            }
            else {
                OrderResponse response = callPrimary( stockSymbol, request );
                if ( response.getStatus().equals( "SUCCESSFUL" ) ) {
                    transactionStatus = "SUCCESSFUL";
                }
            }
        }
        OrderResponse response = OrderResponse
                .newBuilder()
                .setStatus( transactionStatus )
                .build();
        responseObserver.onNext( response );
        responseObserver.onCompleted();
    }

    //get records in the orderBook
    @Override
    public void getOrderBookRecords(Empty request, StreamObserver<OrderRequest> responseObserver) {
        this.server.getOrderBook().forEach( responseObserver::onNext );
        responseObserver.onCompleted();
    }

    //perform trade according to the client request. If a matching order is found, it will be deleted from the orderBook. If not order will be added to orderBook.
    private void performClientRequest() {
        OrderRequest matchOrder = null;
        OrderRequest deleteOrder = null;
        if ( tempDataHolder != null ) {
            String stockSymbol = tempDataHolder.getKey();
            OrderRequest request = tempDataHolder.getValue();
            double price = request.getPrice();
            int quantity = request.getQuantity();
            List<OrderRequest> orderBook = server.getOrderBook();

            //BUY order
            if ( request.getOrderType().equals( "BUY" ) ) {
                for ( OrderRequest orderRequest : orderBook ) {
                    if ( orderRequest.getStockSymbol().equals( stockSymbol ) && orderRequest.getOrderType().equals( "SELL" ) &&
                            orderRequest.getPrice() == price && orderRequest.getQuantity() == quantity ) {
                        matchOrder = orderRequest;
                        break;
                    }
                }
                orderBook.remove( matchOrder );
            }

            //SELL order
            else if ( request.getOrderType().equals( "SELL" ) ) {
                for ( OrderRequest orderRequest : orderBook ) {
                    if ( orderRequest.getStockSymbol().equals( stockSymbol ) && orderRequest.getOrderType().equals( "BUY" ) &&
                            orderRequest.getPrice() == price && orderRequest.getQuantity() == quantity ) {
                        matchOrder = orderRequest;
                        break;
                    }
                }
                orderBook.remove( matchOrder );
            }


            //DELETE order
            else if ( request.getOrderType().equals( "DELETE" ) ) {
                for ( OrderRequest orderRequest : orderBook ) {
                    if ( orderRequest.getId() == request.getId() ) {
                        deleteOrder = orderRequest;
                        break;
                    }
                }
                orderBook.remove( deleteOrder );
            }

            if ( matchOrder == null && deleteOrder == null ) {
                server.addOrderToOrderBook( request );
                System.out.println(stockSymbol + " " + request.getOrderType() +" order request added to the order-book. Committed.. ");
            }
            else if ( matchOrder != null && deleteOrder == null ) {
                System.out.println(stockSymbol + " " + request.getOrderType() +" order request found a match and trade completed. Committed.. " );
            }
            else if ( matchOrder == null && deleteOrder != null ) {
                System.out.println("Order successfully deleted. Committed.. " );
            }
            tempDataHolder = null;
        }
    }

}

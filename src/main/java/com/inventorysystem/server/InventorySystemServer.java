package com.inventorysystem.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.stock.trade.name.service.NameServiceClient;
import com.stock.trade.sycnhronization.DistributedLock;
import com.stock.trade.sycnhronization.DistributedTx;
import com.stock.trade.sycnhronization.DistributedTxCoordinator;
import com.stock.trade.sycnhronization.DistributedTxParticipant;
import com.trade.communication.grpc.generated.OrderRequest;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class InventorySystemServer {
    //name-service
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private final int serverPort;
    //leader
    private final AtomicBoolean isLeader = new AtomicBoolean( false );
    private byte[] leaderData;
    //distributed lock
    private final DistributedLock leaderLock;
    DistributedTx transaction;
    //InventorySystemImpl
    private InventorySystemImpl tradingService = null;
    //registeredStockDb
    private final Map<String, Integer> registeredStockDb = new HashMap<>();
    //orderBook
    private final List<OrderRequest> orderBook = Collections.synchronizedList( new ArrayList<>() );

    {
        // registered stocks
        registeredStockDb.put( "HILTON", 200 );
        registeredStockDb.put( "KINGSBURY", 300 );
        registeredStockDb.put( "CINNAMON", 400 );
        registeredStockDb.put( "JETWING", 500 );
    }

    public InventorySystemServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock( "TradingServerCluster", buildServerData( host, port ) );
        //distributed lock
        tradingService = new InventorySystemImpl( this );
        transaction = new DistributedTxParticipant( tradingService );
    }

    //distributed lock
    public DistributedTx getStockTransaction() {
        return transaction;
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort( serverPort )
                .addService( tradingService )
                .build();
        server.start();
        System.out.println( "Stock Trading Server started and ready to accept requests on port " + serverPort );

        tryToBeLeader();
        server.awaitTermination();
    }

    private void beTheLeader() {
        System.out.println( "I got the leader lock. Now acting as primary" );
        isLeader.set( true );
        transaction = new DistributedTxCoordinator( tradingService );
    }

    public static String buildServerData(String IP, int port) {
        return IP + ":" + port;
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    private synchronized void setCurrentLeaderData(byte[] leaderData) {
        this.leaderData = leaderData;
    }

    private void tryToBeLeader() throws KeeperException, InterruptedException {
        Thread leaderCampaignThread = new Thread( new InventorySystemServer.LeaderCampaignThread() );
        leaderCampaignThread.start();
    }

    class LeaderCampaignThread implements Runnable {
        private byte[] currentLeaderData = null;

        @Override
        public void run() {
            System.out.println( "Starting the leader Campaign" );
            try {
                boolean leader = leaderLock.tryAcquireLock();
                while ( !leader ) {
                    byte[] leaderData = leaderLock.getLockHolderData();
                    if ( currentLeaderData != leaderData ) {
                        currentLeaderData = leaderData;
                        setCurrentLeaderData( currentLeaderData );
                    }
                    Thread.sleep( 10000 );
                    leader = leaderLock.tryAcquireLock();
                }
                currentLeaderData = null;
                beTheLeader();
            }
            catch ( Exception e ) {
                e.printStackTrace();
            }
        }
    }

    public synchronized String[] getCurrentLeaderData() {
        return new String( leaderData ).split( ":" );
    }

    public List<String[]> getOthersData() throws KeeperException, InterruptedException {
        List<String[]> result = new ArrayList<>();
        List<byte[]> othersData = leaderLock.getOthersData();
        for ( byte[] data : othersData ) {
            String[] dataStrings = new String( data ).split( ":" );
            result.add( dataStrings );
        }
        return result;
    }

    public static void main(String[] args) {
        //log4j
        BasicConfigurator.configure();
        Logger.getRootLogger().setLevel( Level.OFF);
        if ( args.length != 1 ) {
            System.out.println( "Usage executable-name <port>" );
        }

        //set the ZooKeeperURL
        DistributedLock.setZooKeeperURL( "localhost:2181" );

        //set the zookeeper url for the distributed transaction
        DistributedTx.setZooKeeperURL( "localhost:2181" );

        //get server port
        int serverPort = 11436;

        InventorySystemServer server = null;
        try {
            //register in name service client (etcd)
            System.out.println("Registered the stock trading server in name service...");
            NameServiceClient client = new NameServiceClient( NAME_SERVICE_ADDRESS );
            client.registerService( "StockTradingService", "127.0.0.1", serverPort, "tcp" );

            //set host and port of the server
            server = new InventorySystemServer( "localhost", serverPort );
            //start server
            server.startServer();
        }
        catch ( InterruptedException | IOException | KeeperException e ) {
            e.printStackTrace();
        }
    }

    //get orderBook
    public List<OrderRequest> getOrderBook() {
        return orderBook;
    }

    //add order request to the orderBook
    public void addOrderToOrderBook(OrderRequest orderRequest) {
        this.orderBook.add( orderRequest );
    }

    //get registered stocks in the system
    public Map<String, Integer> getRegisteredStockDb() {
        return registeredStockDb;
    }

}

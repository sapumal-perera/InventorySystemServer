package com.inventorysystem.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import com.inventorymanager.communication.grpc.generated.InventoryOperationRequest;
import com.inventorysystem.service.NameServiceClient;
import com.inventorysystem.sycnhronization.DistributedLock;
import com.inventorysystem.sycnhronization.DistributedTx;
import com.inventorysystem.sycnhronization.DistributedTxCoordinator;
import com.inventorysystem.sycnhronization.DistributedTxParticipant;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

public class InventorySystemServer {
    public static final String NAME_SERVICE_ADDRESS = "http://localhost:2379";
    private final int serverPort;
    private final AtomicBoolean isLeader = new AtomicBoolean( false );
    private byte[] leaderData;
    private final DistributedLock leaderLock;
    DistributedTx transaction;
    private InventorySystemImpl inventoryManagerService = null;
    private final Map<String, ItemDetail> inventoryDataDb = new HashMap<>();
    private final List<InventoryOperationRequest> orderRequestList = Collections.synchronizedList( new ArrayList<>() );

    {
        inventoryDataDb.put( "TEL-X01", new ItemDetail("TEL-X01", "Telescope X01", 5, 21500));
        inventoryDataDb.put( "DRN-A01",  new ItemDetail("DRN-A01", "Drone A01", 7, 16000));
    }

    public InventorySystemServer(String host, int port) throws InterruptedException, IOException, KeeperException {
        this.serverPort = port;
        leaderLock = new DistributedLock( "TradingServerCluster", buildServerData( host, port ) );
        inventoryManagerService = new InventorySystemImpl( this );
        transaction = new DistributedTxParticipant(inventoryManagerService);
    }

    public DistributedTx getOrderTransaction() {
        return transaction;
    }

    public void startServer() throws IOException, InterruptedException, KeeperException {
        Server server = ServerBuilder
                .forPort( serverPort )
                .addService(inventoryManagerService)
                .build();
        server.start();
        System.out.println( "Inventory system Server started and ready to accept requests on port " + serverPort );

        tryToBeLeader();
        server.awaitTermination();
    }

    private void beTheLeader() {
        System.out.println( "I got the leader lock. Now acting as primary" );
        isLeader.set( true );
        transaction = new DistributedTxCoordinator(inventoryManagerService);
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
        DistributedLock.setZooKeeperURL( "localhost:2181" );
        DistributedTx.setZooKeeperURL( "localhost:2181" );
        int serverPort = Integer.parseInt( args[0] );

        InventorySystemServer server = null;
        try {
            System.out.println("Registered the inventory system server in name service...");
            NameServiceClient client = new NameServiceClient( NAME_SERVICE_ADDRESS );
            client.registerService( "InventoryManagementSystem", "127.0.0.1", serverPort, "tcp" );
            server = new InventorySystemServer( "localhost", serverPort );
            server.startServer();
        }
        catch ( InterruptedException | IOException | KeeperException e ) {
            e.printStackTrace();
        }
    }

    public List<InventoryOperationRequest> getOrderRequestList() {
        return orderRequestList;
    }

    public void addOrderToOrderBook(InventoryOperationRequest orderRequest) {
        this.orderRequestList.add( orderRequest );
    }

    public Map<String, ItemDetail> getInventoryDb() {
        return inventoryDataDb;
    }

}

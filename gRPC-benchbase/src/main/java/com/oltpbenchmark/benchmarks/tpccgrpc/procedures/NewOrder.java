package com.oltpbenchmark.benchmarks.tpccgrpc.procedures;

import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcWorker;
import com.oltpbenchmark.benchmarks.tpccgrpc.procedures.TPCCProcedure;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class NewOrder extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);
    //private TxnServiceGrpc.TxnServiceBlockingStub blockingStub;

    private List<Integer> arrayToList(int[] ints) {
        List<Integer> intList = new ArrayList(ints.length);
        for (int i : ints) {
            intList.add(Integer.valueOf(i));
        }
        return intList;
    }
    
    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub, 
                    Random gen, int terminalWarehouseID, int numWarehouses, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCgrpcWorker w) 
                    throws SQLException, StatusRuntimeException {
        
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtil.getCustomerID(gen);
        
        int numItems = TPCCUtil.randomNumber(5, 15, gen); 
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;

        //int districtItemLowerID = (districtID-1) * 10000 + 1;
        //int districtItemUpperID = districtID * 10000;
        for (int i = 0; i < numItems; i++) {
            // nonuniform distribution: default
            itemIDs[i] = TPCCUtil.getItemID(gen);
            // uniform distribution
            //itemIDs[i] = (int) (gen.nextDouble() * (100000 - 1) + 1);
            // uniform distribution between a fixed range for each district
            //itemIDs[i] = TPCCUtil.randomNumber(districtItemLowerID, districtItemUpperID, gen);

            if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID; // local warehouse
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, numWarehouses, gen);
                }
                while ((supplierWarehouseIDs[i] == terminalWarehouseID) && (numWarehouses > 1));
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10, gen);
        }
        
        // we need to cause 1% of the new orders to be rolled back.
        // rxy: generate a metric to measure actual throughput deleting the aborted transactions
        if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;
        }

        // deprecated grpc implementation: each request will create a new channel and stub
        // String target = null;
        // String target1 = "10.10.1.2:8080";
        // String target2 = "10.10.1.3:8080";

        // if (terminalWarehouseID <= 5) {
        //     target = target1;
        // } else {
        //     target = target2;
        // }

        //ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        //blockingStub = TxnServiceGrpc.newBlockingStub(channel);
        
        int workid = w.getId();
        NewOrderRequest request = NewOrderRequest.newBuilder()
                                    .setTerminalWarehouseID(terminalWarehouseID)
                                    .setDistrictID(districtID)
                                    .setCustomerID(customerID)
                                    .setNumItems(numItems)
                                    .setAllLocal(allLocal)
                                    .addAllItemIDs(arrayToList(itemIDs))
                                    .addAllSupplierWarehouseIDs(arrayToList(supplierWarehouseIDs))
                                    .addAllOrderQuantities(arrayToList(orderQuantities))
                                    .setWorkid(workid)
                                    .build();
        NewOrderReply response;
        // deprecated grpc message implementation: put try...finally... block to shutdown grpc channel
        //try {
        response = blockingStub.newOrderTxn(request);
        //} finally {
        //     try {
        //         channel.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
        //     } catch (Exception e) {
        //         //java Logger
        //         //logger.log(Level.WARNING, "exception thrown: {0}", e);
        //         LOG.warn("exception thrown: {0}", e);
        //     }
        // }
    }
}
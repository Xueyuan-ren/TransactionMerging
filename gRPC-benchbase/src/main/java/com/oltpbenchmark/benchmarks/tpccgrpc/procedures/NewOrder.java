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

        for (int i = 0; i < numItems; i++) {
            // nonuniform distribution: default
            itemIDs[i] = TPCCUtil.getItemID(gen);
            // uniform distribution
            //itemIDs[i] = (int) (gen.nextDouble() * (100000 - 1) + 1);
            
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
        if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfig.INVALID_ITEM_ID;
        }

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
        response = blockingStub.newOrderTxn(request);
    }
}
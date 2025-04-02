/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.oltpbenchmark.benchmarks.tpccgrpc.procedures;

import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcWorker;
import com.oltpbenchmark.benchmarks.tpccgrpc.procedures.TPCCProcedure;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

import java.sql.*;
import java.util.Random;


public class Payment extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Payment.class);

    //private TxnServiceGrpc.TxnServiceBlockingStub blockingStub;

    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub, 
                    Random gen, int w_id, int numWarehouses, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCgrpcWorker w) 
                    throws SQLException, StatusRuntimeException {

        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);

        int x = TPCCUtil.randomNumber(1, 100, gen);

        int customerDistrictID = getCustomerDistrictId(gen, districtID, x);
        int customerWarehouseID = getCustomerWarehouseID(gen, w_id, numWarehouses, x);

        // deprecated grpc implementation: each request will create a new channel and stub
        //String target = "10.10.1.2:8080";
        //ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        //blockingStub = TxnServiceGrpc.newBlockingStub(channel);
        
        int workid = w.getId();
        PaymentRequest request = PaymentRequest.newBuilder()
                                    .setTerminalWarehouseID(w_id)
                                    .setDistrictID(districtID)
                                    .setPaymentAmount(paymentAmount)
                                    .setCustomerDistrictID(customerDistrictID)
                                    .setCustomerWarehouseID(customerWarehouseID)
                                    .setWorkid(workid)
                                    .build();

        PaymentReply response;
        // deprecated grpc message implementation: put try...finally... block to shutdown grpc channel
        //try {
        response = blockingStub.paymentTxn(request);
        // } finally {
        //     try {
        //         channel.shutdownNow().awaitTermination(5L, TimeUnit.SECONDS);
        //     } catch (Exception e) {
        //         //java Logger
        //         //logger.log(Level.WARNING, "exception thrown: {0}", e);
        //         LOG.warn("exception thrown: {0}", e);
        //     }
        // }
        
    }

    private int getCustomerWarehouseID(Random gen, int w_id, int numWarehouses, int x) {
        int customerWarehouseID;
        if (x <= 85) {
            customerWarehouseID = w_id;
        } else {
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            }
            while (customerWarehouseID == w_id && numWarehouses > 1);
        }
        return customerWarehouseID;
    }

    private int getCustomerDistrictId(Random gen, int districtID, int x) {
        if (x <= 85) {
            return districtID;
        } else {
            return TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
        }
    }

}

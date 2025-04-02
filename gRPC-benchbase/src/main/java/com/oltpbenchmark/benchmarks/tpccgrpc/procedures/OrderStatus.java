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

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcWorker;
import com.oltpbenchmark.benchmarks.tpccgrpc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpccgrpc.pojo.Oorder;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class OrderStatus extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatus.class);

    // private TxnServiceGrpc.TxnServiceBlockingStub blockingStub;
    private OrderStatusRequest.Builder requestBuilder = OrderStatusRequest.newBuilder();

    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub, Random gen, int w_id, int numWarehouses, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCgrpcWorker w) 
                    throws SQLException, StatusRuntimeException {

        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int y = TPCCUtil.randomNumber(1, 100, gen);

        boolean c_by_name;
        String c_last = null;
        int c_id = -1;

        if (y <= 60) {
            c_by_name = true;
            c_last = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
            requestBuilder.setCLast(c_last);
        } else {
            c_by_name = false;
            c_id = TPCCUtil.getCustomerID(gen);
            requestBuilder.setCId(c_id);
        }

        //String target = "10.10.1.2:8080";
        int workid = w.getId();
        //ManagedChannel channel = Grpc.newChannelBuilder(target, InsecureChannelCredentials.create()).build();
        //blockingStub = TxnServiceGrpc.newBlockingStub(channel);
        
        OrderStatusRequest request = requestBuilder.setTerminalWarehouseID(w_id)
                                                    .setDistrictID(d_id)
                                                    .setCByName(c_by_name)
                                                    .setWorkid(workid)
                                                    .build();

        OrderStatusReply response;
        //try {
        response = blockingStub.orderStatusTxn(request);
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

}




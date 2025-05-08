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
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpccgrpc.TPCCgrpcWorker;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

import java.math.BigDecimal;
import java.sql.*;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Delivery extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Delivery.class);

    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub, Random gen, int w_id, int numWarehouses, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCgrpcWorker w) 
                    throws SQLException, StatusRuntimeException {

        int o_carrier_id = TPCCUtil.randomNumber(1, 10, gen);
        int workid = w.getId();

        DeliveryRequest request = DeliveryRequest.newBuilder()
                                    .setTerminalWarehouseID(w_id)
                                    .setCarrierID(o_carrier_id)
                                    .setTerminalDistrictUpperID(terminalDistrictUpperID)
                                    .setWorkid(workid)
                                    .build();

        DeliveryReply response;
        response = blockingStub.deliveryTxn(request);
    }

}

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
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class StockLevel extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(StockLevel.class);

    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub, Random gen, int w_id, int numWarehouses, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCgrpcWorker w) 
                    throws SQLException, StatusRuntimeException  {

        int threshold = TPCCUtil.randomNumber(10, 20, gen);
        int d_id = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        int workid = w.getId();
        StockLevelRequest request = StockLevelRequest.newBuilder()
                                    .setTerminalWarehouseID(w_id)
                                    .setDistrictID(d_id)
                                    .setThreshold(threshold)
                                    .setWorkid(workid)
                                    .build();

        StockLevelReply response;
        response = blockingStub.stockLevelTxn(request);
    }

}

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

package com.oltpbenchmark.benchmarks.spreegrpc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeConfig;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeUtil;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeWorker;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

public class NewOrder extends SpreeProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);

    public final SQLStmt stmtCheckNumSQL = new SQLStmt(
        "SELECT 1 AS one FROM spree_orders " +
        "WHERE spree_orders.number = ? LIMIT 1");

    public final SQLStmt stmtCheckBinNumSQL = new SQLStmt(
        "SELECT 1 AS one FROM spree_orders " + 
        "WHERE spree_orders.number = CAST(? AS BINARY) LIMIT 1");
    
    public final SQLStmt stmtInsertOrderSQL = new SQLStmt(
        "INSERT INTO spree_orders (number, state, user_id, email, " +
        "created_at, updated_at, currency, created_by_id, store_id) " +
        "VALUES (?,?,?,?,?,?,?,?,?)");


    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub,
                    Random gen, int terminalWarehouseID, int numWarehouses,
                    int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) 
                    throws SQLException, StatusRuntimeException {

        int districtID = SpreeUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int total_users = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        int user_id = SpreeUtil.randomNumber(1, total_users, gen);
        String email = "spree." + user_id + "@example.com";
        String number = "R" + SpreeUtil.randomNStr(31);
        String state = "cart";
        String currency = "USD";
        int created_by_id = user_id;
        int store_id = 1;
        int workid = w.getId();

        SpreeNewOrderRequest request = SpreeNewOrderRequest.newBuilder()
                                        .setUserId(user_id)
                                        .setEmail(email)
                                        .setNumber(number)
                                        .setState(state)
                                        .setCurrency(currency)
                                        .setCreatedById(created_by_id)
                                        .setStoreId(store_id)
                                        .setWorkid(workid)
                                        .setTerminalWarehouseID(terminalWarehouseID)
                                        .setDistrictID(districtID)
                                        .build();

        SpreeNewOrderReply response;
        response = blockingStub.spreeNewOrderTxn(request);
    }

}
 
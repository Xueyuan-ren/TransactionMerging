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

package com.oltpbenchmark.benchmarks.spreenorpc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeConfig;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeUtil;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Arrays;
import java.util.Random;

public class NewOrder extends SpreeMergedProcedure {

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


    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int mergeSize,
                    int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) throws SQLException {

        int total_users = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        String state = "cart";
        String currency = "USD";
        int store_id = 1;
        int[] user_ids = new int[mergeSize];
        int[] created_by_ids = new int[mergeSize];
        String[] emails = new String[mergeSize];
        String[] numbers = new String[mergeSize];
        int user_id;
        String email = null;
        String number = null;

        for (int i = 0; i < mergeSize; i++) {
            user_id = SpreeUtil.randomNumber(1, total_users, gen);
            email = "spree." + user_id + "@example.com";
            number = "R" + SpreeUtil.randomNStr(31);
            // check number has no duplicate
            while(Arrays.stream(numbers).anyMatch(number::equals)){
                number = "R" + SpreeUtil.randomNStr(31);
            }
            user_ids[i] = user_id;
            created_by_ids[i] = user_id;
            emails[i] = email;
            numbers[i] = number;
        }

        newOrderTransaction(numbers, state, user_ids, emails, 
            currency, created_by_ids, store_id, mergeSize, conn);

    }


    private void newOrderTransaction(String[] numbers, String state, int[] user_ids, String[] emails, 
            String currency, int[] created_by_ids, int store_id, int mergeSize, Connection conn) throws SQLException {

        String stmtCheckNumMTSQL = "SELECT 1 AS one FROM spree_orders " +
                                "WHERE spree_orders.number IN (";

        String stmtCheckBinNumMTSQL = "SELECT 1 AS one FROM spree_orders " + 
                                "WHERE spree_orders.number IN (";

        String stmtInsertOrderMTSQL = "INSERT INTO spree_orders" +
                                    " (number, state, user_id, email, created_at, updated_at, currency, created_by_id, store_id) " +
                                    " VALUES ";

        for (int i = 0; i < mergeSize - 1; i++) {
            stmtCheckNumMTSQL += "?,";
            stmtCheckBinNumMTSQL += "CAST(? AS BINARY),";
            stmtInsertOrderMTSQL += "(?,?,?,?,?,?,?,?,?),";
        }
        stmtCheckNumMTSQL += "?)";
        stmtCheckBinNumMTSQL += "CAST(? AS BINARY))";
        stmtInsertOrderMTSQL += "(?,?,?,?,?,?,?,?,?)";

        SQLStmt stmtCheckNumMT = new SQLStmt(stmtCheckNumMTSQL);
        SQLStmt stmtCheckBinNumMT = new SQLStmt(stmtCheckBinNumMTSQL);
        SQLStmt stmtInsertOrderMT = new SQLStmt(stmtInsertOrderMTSQL);

        try (PreparedStatement stmtCheckNum = this.getPreparedStatement(conn, stmtCheckNumMT);
        PreparedStatement stmtCheckBinNum = this.getPreparedStatement(conn, stmtCheckBinNumMT);
        PreparedStatement stmtInsertOrder = this.getPreparedStatement(conn, stmtInsertOrderMT)) {

            // set order number for each customer
            for (int i = 0; i < mergeSize; i++) {
                stmtCheckNum.setString(i + 1, numbers[i]);
                stmtCheckBinNum.setString(i + 1, numbers[i]);
            }

            // check
            try (ResultSet rs = stmtCheckNum.executeQuery()) {
                if (rs.next()) {
                    throw new UserAbortException("Some of the order numbers have existed!");
                }
            }

            // binary check
            try (ResultSet rs = stmtCheckBinNum.executeQuery()) {
                // if the order number exists (case sensitive)
                if (rs.next()) {
                    throw new UserAbortException("Some of the order numbers (case sensitive) have existed!");
                }
            }

            //insert order
            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
            for (int i = 0; i < mergeSize; i++) {
                stmtInsertOrder.setString(i*9+1, numbers[i]);
                stmtInsertOrder.setString(i*9+2, state);
                stmtInsertOrder.setInt(i*9+3, user_ids[i]);
                stmtInsertOrder.setString(i*9+4, emails[i]);
                stmtInsertOrder.setTimestamp(i*9+5, sysdate);
                stmtInsertOrder.setTimestamp(i*9+6, sysdate);
                stmtInsertOrder.setString(i*9+7, currency);
                stmtInsertOrder.setInt(i*9+8, created_by_ids[i]);
                stmtInsertOrder.setInt(i*9+9, store_id);
            }
            int result = stmtInsertOrder.executeUpdate();
            if (result == 0) {
                LOG.warn("new orders not inserted");
            }
       }

    }
 
}
 
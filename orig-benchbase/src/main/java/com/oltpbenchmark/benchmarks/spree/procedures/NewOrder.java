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

package com.oltpbenchmark.benchmarks.spree.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spree.SpreeConfig;
import com.oltpbenchmark.benchmarks.spree.SpreeUtil;
import com.oltpbenchmark.benchmarks.spree.SpreeWorker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;

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


    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses,
                    int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) throws SQLException {

        int total_users = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        int user_id = SpreeUtil.randomNumber(1, total_users, gen);
        String email = "spree." + user_id + "@example.com";
        String number = "R" + SpreeUtil.randomNStr(31);
        String state = "cart";
        String currency = "USD";
        int created_by_id = user_id;
        int store_id = 1;

        newOrderTransaction(number, state, user_id, email, 
            currency, created_by_id, store_id, conn);

    }


    private void newOrderTransaction(String number, String state, int user_id,
                                     String email, String currency, int created_by_id,
                                     int store_id, Connection conn)
            throws SQLException {

        try (PreparedStatement stmtCheckNum = this.getPreparedStatement(conn, stmtCheckNumSQL);
        PreparedStatement stmtCheckBinNum = this.getPreparedStatement(conn, stmtCheckBinNumSQL);
        PreparedStatement stmtInsertOrder = this.getPreparedStatement(conn, stmtInsertOrderSQL)) {

            // check
            stmtCheckNum.setString(1, number);
            try (ResultSet rs = stmtCheckNum.executeQuery()) {
                // if the order number exists
                if (rs.next()) {
                    //if the number new order rollback
                    throw new UserAbortException("The order number " + number
                               + " has existed!");
                }
            }
            // binary check
            stmtCheckBinNum.setString(1, number);
            try (ResultSet rs = stmtCheckBinNum.executeQuery()) {
                // if the order number exists (case sensitive)
                if (rs.next()) {
                    throw new UserAbortException("The order number (case sensitive) has existed!");
                }
            }

            //insert order 
            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
            stmtInsertOrder.setString(1, number);
            stmtInsertOrder.setString(2, state);
            stmtInsertOrder.setInt(3, user_id);
            stmtInsertOrder.setString(4, email);
            stmtInsertOrder.setTimestamp(5, sysdate);
            stmtInsertOrder.setTimestamp(6, sysdate);
            stmtInsertOrder.setString(7, currency);
            stmtInsertOrder.setInt(8, created_by_id);
            stmtInsertOrder.setInt(9, store_id);
            int result = stmtInsertOrder.executeUpdate();
            if (result == 0) {
                LOG.warn("new order not inserted");
            }
       }

    }
 
}
 
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

package com.oltpbenchmark.benchmarks.tpccnorpc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpccnorpc.TPCCConfig;
import com.oltpbenchmark.benchmarks.tpccnorpc.TPCCConstants;
import com.oltpbenchmark.benchmarks.tpccnorpc.TPCCUtil;
import com.oltpbenchmark.benchmarks.tpccnorpc.TPCCnorpcWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;
import java.util.HashMap;
import java.util.Map;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;
import java.util.stream.*;


public class NewOrder extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);

    //private static final int cus_num = 1;

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
            "SELECT W_TAX " +
            " FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX " +
            " FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            " SET D_NEXT_O_ID = D_NEXT_O_ID + ? " +
            " WHERE D_W_ID = ? " +
            " AND D_ID = ?");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA " +
            " FROM " + TPCCConstants.TABLENAME_ITEM +
            " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
            "S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
            " FROM " + TPCCConstants.TABLENAME_STOCK +
            " WHERE S_I_ID = ? " +
            " AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
            " SET S_QUANTITY = ? , " +
            " S_YTD = S_YTD + ?, " +
            " S_ORDER_CNT = S_ORDER_CNT + 1, " +
            " S_REMOTE_CNT = S_REMOTE_CNT + ? " +
            " WHERE S_I_ID = ? " +
            " AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
            " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
            " VALUES (?,?,?,?,?,?,?,?,?)");


    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int mergeSize, 
                    int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCnorpcWorker w) throws SQLException {

        // districtID of this terminal is randomly determined as a number between lower and upper districtIDs that is belonging to this terminal
        // districtIDs are evenly distributed across all terminals in this warehouse
        // e.g. terminal 1: (1-5 districtIDs), terminal 2:(6-10 districtIDs) if two terminals in this warehouse
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        //int customerID = TPCCUtil.getCustomerID(gen);

        int[] customerIDs = new int[mergeSize];
        int customerID;
        for (int i = 0; i < mergeSize; i++) {
            customerID = TPCCUtil.getCustomerID(gen);
            customerIDs[i] = customerID;
        }
        int[] numItemslist = new int[mergeSize];
        int numItems;
        for (int i = 0; i < mergeSize; i++) {
            numItems = TPCCUtil.randomNumber(5, 15, gen);
            numItemslist[i] = numItems;
        }

        int totalItems = IntStream.of(numItemslist).sum();

        int[] itemIDs = new int[totalItems];
        int[] supplierWarehouseIDs = new int[totalItems];
        int[] orderQuantities = new int[totalItems];
        int[] allLocal = new int[mergeSize];
        Arrays.fill(allLocal, 1);

        int j = 0;
        int temp = 0;
        for (int i = 0; i < mergeSize; i++) {
            numItems = numItemslist[i] + j;
            for (j = temp; j < numItems; j++) {
                temp++;
                // nonuniform distribution: default
                itemIDs[j] = TPCCUtil.getItemID(gen);
                // uniform distribution
                //itemIDs[j] = (int) (gen.nextDouble() * (100000 - 1) + 1);

                // cross warehouse
                if (TPCCUtil.randomNumber(1, 100, gen) > 1) {
                    supplierWarehouseIDs[j] = terminalWarehouseID; // item in local warehouse (the same as the terminal's warehouseID)
                } else {
                    do {
                        supplierWarehouseIDs[j] = TPCCUtil.randomNumber(1, numWarehouses, gen);  // item probably in other warehouses 
                    } while (supplierWarehouseIDs[j] == terminalWarehouseID
                            && numWarehouses > 1);  // make this item in remote warehouses if numWarehouses > 1
                    allLocal[i] = 0;
                }
 
                orderQuantities[j] = TPCCUtil.randomNumber(1, 10, gen);
            }
        }

        int invalid_flag = 0;
        // we need to cause 1% of the new orders to be rolled back (totally 1% * mergeSize).
        for (int i = 0; i < mergeSize; i++) {
            if (TPCCUtil.randomNumber(1, 100, gen) == 1) {
                itemIDs[totalItems - 1] = TPCCConfig.INVALID_ITEM_ID;
                invalid_flag = 1;
            }
        }

        newOrderTransaction(invalid_flag, terminalWarehouseID, districtID,
                customerIDs, totalItems, numItemslist, allLocal, itemIDs,
                supplierWarehouseIDs, orderQuantities, mergeSize, conn);

    }


    private void newOrderTransaction(int invalid_flag, int w_id, int d_id, int[] c_id,
                                     int totalItems, int[] numItemslist, int[] o_all_local, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities, int mergeSize, Connection conn)
            throws SQLException {
        Map<Integer, Float> item_price = new HashMap<>();
        Map<Entry<Integer, Integer>, Integer> item_squantity = new HashMap<>();
        Map<Entry<Integer, Integer>, Integer> item_olquantity = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_01 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_02 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_03 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_04 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_05 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_06 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_07 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_08 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_09 = new HashMap<>();
        Map<Entry<Integer, Integer>, String> item_s_dist_10 = new HashMap<>();
        Map<Entry<Integer, Integer>, Integer> s_remote_cnt_increments = new HashMap<>();
        float i_price;
        int d_next_o_id;
        int o_id;
        int s_quantity;
        String s_dist_01 = null;
        String s_dist_02 = null;
        String s_dist_03 = null;
        String s_dist_04 = null;
        String s_dist_05 = null;
        String s_dist_06 = null;
        String s_dist_07 = null;
        String s_dist_08 = null;
        String s_dist_09 = null;
        String s_dist_10 = null;
        String ol_dist_info = "null";

        int ol_supply_w_id;
        int ol_i_id;
        int s_w_id;
        int ol_quantity;
        int s_remote_cnt_increment;
        float ol_amount;
        int item_size = 0;
        int temp = 0;
        
        // verify itemID and warehouseID pairs have no repeat values: 
        ArrayList<Entry<Integer, Integer>> w_item = new ArrayList<>();
        for (int i = 0; i < totalItems; i++) {
            Entry<Integer, Integer> pair = new SimpleEntry<>(supplierWarehouseIDs[i], itemIDs[i]);
            if (!w_item.contains(pair)) {
                w_item.add(pair);
            }
        }

        int nondup = w_item.size();
        
        String stmtGetCustMXSQL = "SELECT C_DISCOUNT, C_LAST, C_CREDIT, C_ID" +
                                  " FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
                                  " WHERE C_W_ID = ? " +
                                  " AND C_D_ID = ? " + 
                                  " AND C_ID IN (";
        String stmtInsertNewOrderMXSQL = "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                                         " (NO_O_ID, NO_D_ID, NO_W_ID) " +
                                         " VALUES ";
        String stmtInsertOOrderMXSQL = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                                       " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                                       " VALUES ";
        
        String stmtGetItemMXSQL = "SELECT I_ID, I_PRICE, I_NAME , I_DATA " +
                                  " FROM " + TPCCConstants.TABLENAME_ITEM +
                                  " WHERE I_ID IN (";

        String stmtGetStockMXSQL = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05," +
                                   " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                                   " FROM " + TPCCConstants.TABLENAME_STOCK +
                                   " WHERE (S_W_ID, S_I_ID) IN (";
                                   //     S_W_ID = ? AND S_I_ID = ? 
                                   // OR  S_W_ID = ? AND S_I_ID = ? 
                                   // OR  ......
                                   // version 1: WHERE (S_W_ID, S_I_ID) IN ((?,?),(?,?),(?,?),(?,?),(?,?),...);

        String stmtInsertOrderLineMXSQL = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                                          " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
                                          " VALUES ";

        String stmtUpdateStockMXSQL = "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                                      " SET S_QUANTITY = CASE ";
                                     // "               WHEN S_W_ID = ? AND S_I_ID = ? THEN ? " +
                                     // "               ELSE S_QUANTITY END, " +
                                     // "       S_YTD = CASE " +
                                     // "               WHEN S_W_ID = ? AND S_I_ID = ? THEN S_YTD + ? " +
                                     // "               ELSE S_YTD END, " +
                                     // "       S_ORDER_CNT = CASE " +
                                     // "               WHEN S_W_ID = ? AND S_I_ID = ? THEN S_ORDER_CNT + 1 " +
                                     // "               ELSE S_ORDER_CNT END, " +
                                     // "       S_REMOTE_CNT = CASE " +
                                     // "               WHEN S_W_ID = ? AND S_I_ID = ? THEN S_REMOTE_CNT + ? " +
                                     // "               ELSE S_REMOTE_CNT END " +
                                     // "  WHERE S_I_ID IN (?) AND S_W_ID = ?";
                                     // version 1: WHERE (S_W_ID, S_I_ID) IN ((?,?),(?,?),(?,?),(?,?),(?,?),...);

        // concatenate SQL strings with certain number of specific '?' strings
        // and make as SQLStmt objects
        for (int i = 0; i < mergeSize - 1; i++) {
            stmtGetCustMXSQL += "?,";
            stmtInsertNewOrderMXSQL += "(?, ?, ?),";
            stmtInsertOOrderMXSQL += "(?, ?, ?, ?, ?, ?, ?),";
        }
        stmtGetCustMXSQL += "?)";
        stmtInsertNewOrderMXSQL += "(?, ?, ?)";
        stmtInsertOOrderMXSQL += "(?, ?, ?, ?, ?, ?, ?)";

        // paremeter: (2 + 2 + 1 + 2) * o_ol_cnt + 1 = 7 * o_ol_cnt + 1
        for (int i = 0; i < mergeSize; i++) {
            for (int j = 0; j < numItemslist[i]; j++) {
                stmtInsertOrderLineMXSQL += "(?,?,?,?,?,?,?,?,?),";
                stmtGetItemMXSQL += "?,";
                //stmtGetStockMXSQL += "S_W_ID = ? AND S_I_ID = ? OR ";
                stmtGetStockMXSQL += "(?,?),";
            }
        }

        // delete the last ',' in stmtInsertOrderLineMXSQL/stmtGetItemMXSQL
        stmtInsertOrderLineMXSQL = stmtInsertOrderLineMXSQL.substring(0, stmtInsertOrderLineMXSQL.length() - 1);
        stmtGetItemMXSQL = stmtGetItemMXSQL.substring(0, stmtGetItemMXSQL.length() - 1);
        stmtGetItemMXSQL += ")";

        stmtGetStockMXSQL = stmtGetStockMXSQL.substring(0, stmtGetStockMXSQL.length() - 1);
        stmtGetStockMXSQL += ") FOR UPDATE";

        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMXSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN ? ";
        }
        stmtUpdateStockMXSQL += " ELSE S_QUANTITY END, S_YTD = CASE ";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMXSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_YTD + ? ";
        }
        stmtUpdateStockMXSQL += " ELSE S_YTD END, S_ORDER_CNT = CASE ";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMXSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_ORDER_CNT + 1 ";
        }
        stmtUpdateStockMXSQL += " ELSE S_ORDER_CNT END, S_REMOTE_CNT = CASE ";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMXSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_REMOTE_CNT + ? ";
        }
        stmtUpdateStockMXSQL += " ELSE S_REMOTE_CNT END WHERE (S_W_ID, S_I_ID) IN (";

        for (int i = 0; i < nondup - 1; i++) {
            stmtUpdateStockMXSQL += "(?,?),";
        }
        stmtUpdateStockMXSQL += "(?,?))";

        SQLStmt stmtGetCustMX = new SQLStmt(stmtGetCustMXSQL);
        SQLStmt stmtInsertNewOrderMX = new SQLStmt(stmtInsertNewOrderMXSQL);
        SQLStmt stmtInsertOOrderMX = new SQLStmt(stmtInsertOOrderMXSQL);
        SQLStmt stmtGetItemMX = new SQLStmt(stmtGetItemMXSQL);
        SQLStmt stmtGetStockMX = new SQLStmt(stmtGetStockMXSQL);
        SQLStmt stmtInsertOrderLineMX = new SQLStmt(stmtInsertOrderLineMXSQL);
        SQLStmt stmtUpdateStockMX = new SQLStmt(stmtUpdateStockMXSQL);

        try (
            PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustMX);
            PreparedStatement stmtGetWhse = this.getPreparedStatement(conn, stmtGetWhseSQL);
            PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL);
            PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderMX);
            PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL);
            PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderMX);
            PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemMX);
            PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockMX);
            PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockMX);
            PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineMX)
            ) {

            
                stmtGetCust.setInt(1, w_id);
                stmtGetCust.setInt(2, d_id);
                //stmtGetCust.setInt(3, c_id);

                // set each customerID
                for (int i = 3; i < mergeSize + 3; i++) {
                       stmtGetCust.setInt(i, c_id[i-3]);
                }

                try (ResultSet rs = stmtGetCust.executeQuery()) {
                    while (!rs.next()) {
                        throw new RuntimeException("C_D_ID=" + d_id + " C_ID= " + rs.getInt("C_ID") + " not found!");
                    }
                }

                stmtGetWhse.setInt(1, w_id);

                try (ResultSet rs = stmtGetWhse.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("W_ID=" + w_id + " not found!");
                    }
                }

                stmtGetDist.setInt(1, w_id);
                stmtGetDist.setInt(2, d_id);

                try (ResultSet rs = stmtGetDist.executeQuery()) {
                    if (!rs.next()) {
                        throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                    }
                    d_next_o_id = rs.getInt("D_NEXT_O_ID");
                }

                //update next_order_id first, but it might doesn't matter 
                stmtUpdateDist.setInt(1, mergeSize);
                stmtUpdateDist.setInt(2, w_id);
                stmtUpdateDist.setInt(3, d_id);

                int result = stmtUpdateDist.executeUpdate();

                if (result == 0) {
                    throw new RuntimeException(
                           "Error!! Cannot update next_order_id on district for D_ID="
                                   + d_id + " D_W_ID=" + w_id);
                }

                o_id = d_next_o_id;

                // insert an oorder for each customer
                for (int i = 0; i < mergeSize; i++) {
                    stmtInsertOOrder.setInt(i*7 +1, o_id + i);
                    stmtInsertOOrder.setInt(i*7 +2, d_id);
                    stmtInsertOOrder.setInt(i*7 +3, w_id);
                    stmtInsertOOrder.setInt(i*7 +4, c_id[i]);
                    stmtInsertOOrder.setTimestamp(i*7 +5, new Timestamp(System.currentTimeMillis()));
                    stmtInsertOOrder.setInt(i*7 +6, numItemslist[i]);
                    stmtInsertOOrder.setInt(i*7 +7, o_all_local[i]);
                }

                stmtInsertOOrder.executeUpdate();
                
                // insert new order
                for (int i = 0; i < mergeSize; i++) {
                    stmtInsertNewOrder.setInt(i*3 +1, o_id + i);
                    stmtInsertNewOrder.setInt(i*3 +2, d_id);
                    stmtInsertNewOrder.setInt(i*3 +3, w_id);
                }
                
                stmtInsertNewOrder.executeUpdate();

                int ol_number = 0, numItems = 0;
                temp = 0;
                // unwrap inner loop for each customer
                for (int cus_idx = 0; cus_idx < mergeSize; cus_idx++) {
                    numItems = numItemslist[cus_idx] + ol_number;
                    for (ol_number = temp; ol_number < numItems; ol_number++) {
                        temp++;
                        ol_supply_w_id = supplierWarehouseIDs[ol_number];
                        ol_i_id = itemIDs[ol_number];
                        stmtGetItem.setInt(ol_number+1, ol_i_id);

                        stmtGetStock.setInt(ol_number*2 + 1, ol_supply_w_id);
                        stmtGetStock.setInt(ol_number*2 + 2, ol_i_id);
                    }
                }
                
                // first execute GetItem and GetStock to get i_price and s_quantity of each item
                try (ResultSet rs = stmtGetItem.executeQuery()) {
                    if (invalid_flag == 1) {
                        // This is (hopefully) an expected error: this is an
                        // expected new order rollback
                        throw new UserAbortException(
                                   "EXPECTED new order rollback: invalid item id in items, id not found!");
                    } else {
                        while (rs.next()) {
                            ol_i_id = rs.getInt("I_ID");
                            i_price = rs.getFloat("I_PRICE");
                            item_price.put(ol_i_id, i_price);
                        }
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                
                try (ResultSet rs = stmtGetStock.executeQuery()) {
                    while (rs.next()) {
                        s_w_id = rs.getInt("S_W_ID");
                        ol_i_id = rs.getInt("S_I_ID");
                        s_quantity = rs.getInt("S_QUANTITY");
                        s_dist_01 = rs.getString("S_DIST_01");
                        s_dist_02 = rs.getString("S_DIST_02");
                        s_dist_03 = rs.getString("S_DIST_03");
                        s_dist_04 = rs.getString("S_DIST_04");
                        s_dist_05 = rs.getString("S_DIST_05");
                        s_dist_06 = rs.getString("S_DIST_06");
                        s_dist_07 = rs.getString("S_DIST_07");
                        s_dist_08 = rs.getString("S_DIST_08");
                        s_dist_09 = rs.getString("S_DIST_09");
                        s_dist_10 = rs.getString("S_DIST_10");

                        Entry<Integer, Integer> key = new SimpleEntry<>(s_w_id, ol_i_id);
                        item_squantity.put(key, s_quantity);
                        item_s_dist_01.put(key, s_dist_01);
                        item_s_dist_02.put(key, s_dist_02);
                        item_s_dist_03.put(key, s_dist_03);
                        item_s_dist_04.put(key, s_dist_04);
                        item_s_dist_05.put(key, s_dist_05);
                        item_s_dist_06.put(key, s_dist_06);
                        item_s_dist_07.put(key, s_dist_07);
                        item_s_dist_08.put(key, s_dist_08);
                        item_s_dist_09.put(key, s_dist_09);
                        item_s_dist_10.put(key, s_dist_10);
                    }
                }

                // update the s_quantity for each item and set parameters for each order line
                int currentQua = 0;

                ol_number = 0;
                numItems = 0;
                temp = 0;
                for (int cus_idx = 0; cus_idx < mergeSize; cus_idx++) {
                    numItems = numItemslist[cus_idx] + ol_number;
                    int ol_index = 0;
                    for (ol_number = temp; ol_number < numItems; ol_number++) {
                        temp++;
                        ol_index += 1;
                        ol_supply_w_id = supplierWarehouseIDs[ol_number];
                        ol_i_id = itemIDs[ol_number];
                        Entry<Integer, Integer> key = new SimpleEntry<>(ol_supply_w_id, ol_i_id);
                        ol_quantity = orderQuantities[ol_number];
                        ol_amount = ol_quantity * (item_price.get(ol_i_id));
                        switch (d_id) {
                            case 1:
                                ol_dist_info = item_s_dist_01.get(key);
                                break;
                            case 2:
                                ol_dist_info = item_s_dist_02.get(key);
                                break;
                            case 3:
                                ol_dist_info = item_s_dist_03.get(key);
                                break;
                            case 4:
                                ol_dist_info = item_s_dist_04.get(key);
                                break;
                            case 5:
                                ol_dist_info = item_s_dist_05.get(key);
                                break;
                            case 6:
                                ol_dist_info = item_s_dist_06.get(key);
                                break;
                            case 7:
                                ol_dist_info = item_s_dist_07.get(key);
                                break;
                            case 8:
                                ol_dist_info = item_s_dist_08.get(key);
                                break;
                            case 9:
                                ol_dist_info = item_s_dist_09.get(key);
                                break;
                            case 10:
                                ol_dist_info = item_s_dist_10.get(key);
                                break;
                        }
                        // set parameters for each insert order line
                        stmtInsertOrderLine.setInt(ol_number * 9 + 1, o_id + cus_idx);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 2, d_id);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 3, w_id);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 4, ol_index);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 5, ol_i_id);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 6, ol_supply_w_id);
                        stmtInsertOrderLine.setInt(ol_number * 9 + 7, ol_quantity);
                        stmtInsertOrderLine.setDouble(ol_number * 9 + 8, ol_amount);
                        stmtInsertOrderLine.setString(ol_number * 9 + 9, ol_dist_info);

                        // update the ol_quantity for each item
                        if (item_olquantity.containsKey(key)) {
                            currentQua = item_olquantity.get(key);
                            // add ol_quantity to existing item
                            item_olquantity.put(key, currentQua + ol_quantity);
                        } else {
                            item_olquantity.put(key, ol_quantity);
                        }

                        //update the s_quantity for each item
                        s_quantity = item_squantity.get(key);
                        if (s_quantity - ol_quantity >= 10) {
                            s_quantity -= ol_quantity;
                        } else {
                            s_quantity += -ol_quantity + 91;
                        }
                        item_squantity.put(key, s_quantity);

                        // set the remote warehouse count for each item
                        if (ol_supply_w_id == w_id) {
                            s_remote_cnt_increments.put(key, 0);
                        } else {
                            s_remote_cnt_increments.put(key, 1);
                        }
                    }
                 }


                item_size = nondup;
                for (int num = 0; num < nondup; num++) {
                    Entry<Integer, Integer> pairkey = w_item.get(num);
                    s_w_id = pairkey.getKey();
                    ol_i_id = pairkey.getValue();
                    s_quantity = item_squantity.get(pairkey);
                    ol_quantity = item_olquantity.get(pairkey);
                    s_remote_cnt_increment = s_remote_cnt_increments.get(pairkey);
                    
                    stmtUpdateStock.setInt(num * 3 + 1, s_w_id);
                    stmtUpdateStock.setInt(num * 3 + 2, ol_i_id);
                    stmtUpdateStock.setInt(num * 3 + 3, s_quantity);

                    stmtUpdateStock.setInt(num * 3 + item_size*3 + 1, s_w_id);
                    stmtUpdateStock.setInt(num * 3 + item_size*3 + 2, ol_i_id);
                    stmtUpdateStock.setInt(num * 3 + item_size*3 + 3, ol_quantity);
 
                    stmtUpdateStock.setInt(num * 2 + item_size*3 + item_size*3 + 1, s_w_id);
                    stmtUpdateStock.setInt(num * 2 + item_size*3 + item_size*3 + 2, ol_i_id);

                    stmtUpdateStock.setInt(num * 3 + item_size*3 + item_size*3 + item_size*2 + 1, s_w_id);
                    stmtUpdateStock.setInt(num * 3 + item_size*3 + item_size*3 + item_size*2 + 2, ol_i_id);
                    stmtUpdateStock.setInt(num * 3 + item_size*3 + item_size*3 + item_size*2 + 3, s_remote_cnt_increment);

                    stmtUpdateStock.setInt(num * 2 + item_size*3 + item_size*3 + item_size*2 + item_size*3 + 1, s_w_id);
                    stmtUpdateStock.setInt(num * 2 + item_size*3 + item_size*3 + item_size*2 + item_size*3 + 2, ol_i_id);
                }
                
                stmtInsertOrderLine.executeUpdate();
                stmtUpdateStock.executeUpdate();
        }   
    }
 
}
 
package com.grpc.examples.tpcc;

import com.grpc.examples.Procedure;
import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.tpcc.pojo.Stock;
import com.grpc.examples.NewOrderRequest;
import com.grpc.examples.NewOrderReply;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.Map.Entry;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewOrderMT extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrderMT.class);

    private NewOrderReply.Builder response = NewOrderReply.newBuilder();

    private int MERGE_SIZE;

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
            " FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ?" +
            " AND C_D_ID = ?" +
            " AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
            "SELECT W_TAX" +
            " FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX" +
            " FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
            " (NO_O_ID, NO_D_ID, NO_W_ID)" +
            " VALUES ( ?, ?, ?)");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            " SET D_NEXT_O_ID = D_NEXT_O_ID + ?" +
            " WHERE D_W_ID = ?" +
            " AND D_ID = ?");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
            " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA" +
            " FROM " + TPCCConstants.TABLENAME_ITEM +
            " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05," +
            " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
            " FROM " + TPCCConstants.TABLENAME_STOCK +
            " WHERE S_I_ID = ?" +
            " AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
            " SET S_QUANTITY = ? ," +
            " S_YTD = S_YTD + ?," +
            " S_ORDER_CNT = S_ORDER_CNT + 1," +
            " S_REMOTE_CNT = S_REMOTE_CNT + ?" +
            " WHERE S_I_ID = ?" +
            " AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
            " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)" +
            " VALUES (?,?,?,?,?,?,?,?,?)");
    
    public NewOrderMT(int merge) {
        MERGE_SIZE = merge;
        //System.out.println("NewOrderMT MERGE_SIZE: " + MERGE_SIZE);
    }

    public NewOrderReply newOrderMergeTransaction(NewOrderRequest[] requests, Connection conn) throws SQLException {

        int w_id = requests[0].getTerminalWarehouseID();
        int d_id = requests[0].getDistrictID();
        int[] c_ids = new int[MERGE_SIZE];
        int[] numItemslist = new int[MERGE_SIZE];
        int[] allLocals = new int[MERGE_SIZE];
        for (int i = 0; i < MERGE_SIZE; i++) {
            int c_id = requests[i].getCustomerID();
            int o_ol_cnt = requests[i].getNumItems();
            int o_all_local = requests[i].getAllLocal();
			c_ids[i] = c_id;
            numItemslist[i] = o_ol_cnt;
            allLocals[i] = o_all_local;
		}

        int totalItems = IntStream.of(numItemslist).sum();
        int invalid_flag = 0;
        int[] itemIDs = new int[totalItems];
        int[] supplierWarehouseIDs = new int[totalItems];
        int[] orderQuantities = new int[totalItems];
        List<Integer> itemIDList = new ArrayList<Integer>();
        List<Integer> supplierWarehouseIDList = new ArrayList<Integer>();
        List<Integer> orderQuantityList = new ArrayList<Integer>();
        for (int i = 0; i < MERGE_SIZE; i++) {
            itemIDList.addAll(requests[i].getItemIDsList());
            supplierWarehouseIDList.addAll(requests[i].getSupplierWarehouseIDsList());
            orderQuantityList.addAll(requests[i].getOrderQuantitiesList());
        }
        itemIDs = itemIDList.stream().mapToInt(Integer::intValue).toArray();
        supplierWarehouseIDs = supplierWarehouseIDList.stream().mapToInt(Integer::intValue).toArray();
        orderQuantities = orderQuantityList.stream().mapToInt(Integer::intValue).toArray();
        // verify itemIDs have invalid values
        // and verify itemID and warehouseID pairs have no repeat values
        ArrayList<Entry<Integer, Integer>> w_item = new ArrayList<>();
        for (int i = 0; i < totalItems; i++) {
            if (itemIDs[i] == TPCCConfig.INVALID_ITEM_ID) {
                invalid_flag = 1;
            }
            Entry<Integer, Integer> pair = new SimpleEntry<>(supplierWarehouseIDs[i], itemIDs[i]);
            if (!w_item.contains(pair)) {
                w_item.add(pair);
            }
        }
        int nondup = w_item.size();

        String stmtGetCustMTSQL = "SELECT C_DISCOUNT, C_LAST, C_CREDIT, C_ID" +
                                  " FROM " + TPCCConstants.TABLENAME_CUSTOMER + 
                                  " WHERE C_W_ID = ?" +
                                  " AND C_D_ID = ?" + 
                                  " AND C_ID IN (";

        String stmtInsertNewOrderMTSQL = "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
                                         " (NO_O_ID, NO_D_ID, NO_W_ID)" +
                                         " VALUES ";
        String stmtInsertOOrderMTSQL = "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
                                       " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
                                       " VALUES ";
        
        String stmtGetItemMTSQL = "SELECT I_ID, I_PRICE, I_NAME , I_DATA" +
                                  " FROM " + TPCCConstants.TABLENAME_ITEM +
                                  " WHERE I_ID IN (";

        String stmtGetStockMTSQL = "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05," +
                                   " S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
                                   " FROM " + TPCCConstants.TABLENAME_STOCK +
                                   " WHERE (S_W_ID, S_I_ID) IN (";

        String stmtInsertOrderLineMTSQL = "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
                                          " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO)" +
                                          " VALUES ";

        String stmtUpdateStockMTSQL = "UPDATE " + TPCCConstants.TABLENAME_STOCK +
                                      " SET S_QUANTITY = CASE";
		
        // concatenate SQL strings with certain number of specific '?'
        for (int i = 0; i < MERGE_SIZE - 1; i++) {
			stmtGetCustMTSQL += "?,";
            stmtInsertNewOrderMTSQL += "(?, ?, ?),";
            stmtInsertOOrderMTSQL += "(?, ?, ?, ?, ?, ?, ?),";
        }
        stmtGetCustMTSQL += "?)";
        stmtInsertNewOrderMTSQL += "(?, ?, ?)";
        stmtInsertOOrderMTSQL += "(?, ?, ?, ?, ?, ?, ?)";
        // paremeter: (2 + 2 + 1 + 2) * o_ol_cnt + 1 = 7 * o_ol_cnt + 1
        for (int i = 0; i < MERGE_SIZE; i++) {
            for (int j = 0; j < numItemslist[i]; j++) {
                stmtInsertOrderLineMTSQL += "(?,?,?,?,?,?,?,?,?),";
                stmtGetItemMTSQL += "?,";
                stmtGetStockMTSQL += "(?,?),";
            }
        }
        // delete the last ',' in stmtInsertOrderLineMTSQL/stmtGetItemMTSQL
        stmtInsertOrderLineMTSQL = stmtInsertOrderLineMTSQL.substring(0, stmtInsertOrderLineMTSQL.length() - 1);
        stmtGetItemMTSQL = stmtGetItemMTSQL.substring(0, stmtGetItemMTSQL.length() - 1);
        stmtGetItemMTSQL += ")";
        stmtGetStockMTSQL = stmtGetStockMTSQL.substring(0, stmtGetStockMTSQL.length() - 1);
        stmtGetStockMTSQL += ") FOR UPDATE";

        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMTSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN ?";
        }
        stmtUpdateStockMTSQL += " ELSE S_QUANTITY END, S_YTD = CASE";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMTSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_YTD + ?";
        }
        stmtUpdateStockMTSQL += " ELSE S_YTD END, S_ORDER_CNT = CASE";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMTSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_ORDER_CNT + 1";
        }
        stmtUpdateStockMTSQL += " ELSE S_ORDER_CNT END, S_REMOTE_CNT = CASE";
        for (int i = 0; i < nondup; i++) {
            stmtUpdateStockMTSQL += " WHEN S_W_ID = ? AND S_I_ID = ? THEN S_REMOTE_CNT + ?";
        }
        stmtUpdateStockMTSQL += " ELSE S_REMOTE_CNT END WHERE (S_W_ID, S_I_ID) IN (";

        for (int i = 0; i < nondup - 1; i++) {
            stmtUpdateStockMTSQL += "(?,?),";
        }
        stmtUpdateStockMTSQL += "(?,?))";

        SQLStmt stmtGetCustMT = new SQLStmt(stmtGetCustMTSQL);
        SQLStmt stmtInsertNewOrderMT = new SQLStmt(stmtInsertNewOrderMTSQL);
        SQLStmt stmtInsertOOrderMT = new SQLStmt(stmtInsertOOrderMTSQL);
        SQLStmt stmtGetItemMT = new SQLStmt(stmtGetItemMTSQL);
        SQLStmt stmtGetStockMT = new SQLStmt(stmtGetStockMTSQL);
        SQLStmt stmtInsertOrderLineMT = new SQLStmt(stmtInsertOrderLineMTSQL);
        SQLStmt stmtUpdateStockMT = new SQLStmt(stmtUpdateStockMTSQL);

        getCustomer(conn, w_id, d_id, c_ids, stmtGetCustMT);

        getWarehouse(conn, w_id);

        int d_next_o_id = getDistrict(conn, w_id, d_id);

        updateDistrict(conn, w_id, d_id);

        insertOpenOrder(conn, w_id, d_id, c_ids, numItemslist, allLocals, d_next_o_id, stmtInsertOOrderMT);

        insertNewOrder(conn, w_id, d_id, d_next_o_id, stmtInsertNewOrderMT);

        try (PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemMT);
             PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockMT);
             PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockMT);
             PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineMT)) {

            // unwrap inner loop for each customer
            int ol_number = 0, numItems = 0, temp = 0;
            for (int cus_idx = 0; cus_idx < MERGE_SIZE; cus_idx++) {
                numItems = numItemslist[cus_idx] + ol_number;
                for (ol_number = temp; ol_number < numItems; ol_number++) {
                    temp++;
                    int ol_supply_w_id = supplierWarehouseIDs[ol_number];
                    int ol_i_id = itemIDs[ol_number];
                    stmtGetItem.setInt(ol_number+1, ol_i_id);
                    stmtGetStock.setInt(ol_number*2 + 1, ol_supply_w_id);
                    stmtGetStock.setInt(ol_number*2 + 2, ol_i_id);
                }
            }
            // first execute GetItem and GetStock to get i_price and s_quantity of each item
            // this may occasionally error and that's ok!
            Map<Integer, Float> item_price = new HashMap<>();
            item_price = getItemPrice(conn, invalid_flag, stmtGetItem);
            
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
            int ol_i_id, s_w_id, s_quantity, ol_quantity, s_remote_cnt_increment;
            float ol_amount;
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
            int ol_supply_w_id = 0;
            ol_number = 0;
            numItems = 0;
            temp = 0;
            for (int cus_idx = 0; cus_idx < MERGE_SIZE; cus_idx++) {
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
                        stmtInsertOrderLine.setInt(ol_number * 9 + 1, d_next_o_id + cus_idx);
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
            // set parameters for all distinct order lines(items) in update stock txn
            int item_size = nondup;
            for (int num = 0; num < nondup; num++) {
                Entry<Integer, Integer> pairkey = w_item.get(num);
                s_w_id = pairkey.getKey();
                ol_i_id = pairkey.getValue();
                s_quantity = item_squantity.get(pairkey);
                //s_quantity = 15;
                ol_quantity = item_olquantity.get(pairkey);
                s_remote_cnt_increment = s_remote_cnt_increments.get(pairkey);
                //s_remote_cnt_increment = 0;
                
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
            
            // int[] rsinsol = stmtInsertOrderLine.executeBatch();
            // response.addAllInsol(arrayToList(rsinsol));
            // stmtInsertOrderLine.clearBatch();

            // int[] rsupdst = stmtUpdateStock.executeBatch();
            // response.addAllUpdst(arrayToList(rsupdst));
            // stmtUpdateStock.clearBatch();

        }
        response.setCompleted(true);
        return response.build();

    }

    private String getDistInfo(int d_id, Stock s) {
        return switch (d_id) {
            case 1 -> s.s_dist_01;
            case 2 -> s.s_dist_02;
            case 3 -> s.s_dist_03;
            case 4 -> s.s_dist_04;
            case 5 -> s.s_dist_05;
            case 6 -> s.s_dist_06;
            case 7 -> s.s_dist_07;
            case 8 -> s.s_dist_08;
            case 9 -> s.s_dist_09;
            case 10 -> s.s_dist_10;
            default -> null;
        };
    }

    private List<Stock> getStock(Connection conn, int nondup, PreparedStatement stmtGetStock) throws SQLException {
       List<Stock> stocks = new ArrayList<Stock>(nondup);
        try (ResultSet rs = stmtGetStock.executeQuery()) {
            while (rs.next()) {
                Stock s = new Stock();
                s.s_w_id = rs.getInt("S_W_ID");
                s.s_i_id = rs.getInt("S_I_ID");
                s.s_quantity = rs.getInt("S_QUANTITY");
                s.s_dist_01 = rs.getString("S_DIST_01");
                s.s_dist_02 = rs.getString("S_DIST_02");
                s.s_dist_03 = rs.getString("S_DIST_03");
                s.s_dist_04 = rs.getString("S_DIST_04");
                s.s_dist_05 = rs.getString("S_DIST_05");
                s.s_dist_06 = rs.getString("S_DIST_06");
                s.s_dist_07 = rs.getString("S_DIST_07");
                s.s_dist_08 = rs.getString("S_DIST_08");
                s.s_dist_09 = rs.getString("S_DIST_09");
                s.s_dist_10 = rs.getString("S_DIST_10");
                stocks.add(s);
            }
            return stocks;
        }
    }

    private Map<Integer, Float> getItemPrice(Connection conn, int flag, PreparedStatement stmtGetItem) throws SQLException {
        Map<Integer, Float> item_price = new HashMap<>();
        try (ResultSet rs = stmtGetItem.executeQuery()) {
                if (flag == 1) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException("EXPECTED new order rollback: invalid item id in items, itemID not found!");
                } else {
                    while (rs.next()) {
                        int ol_i_id = rs.getInt("I_ID");
                        float i_price = rs.getFloat("I_PRICE");
                        item_price.put(ol_i_id, i_price);
                        //distinct_items++;
                    }
                    return item_price;
                }
        }
        
    }

    private void insertNewOrder(Connection conn, int w_id, int d_id, int o_id, SQLStmt stmtInsertNewOrderMT) throws SQLException {
        try (PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderMT)) {
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtInsertNewOrder.setInt(i*3 +1, o_id + i);
                stmtInsertNewOrder.setInt(i*3 +2, d_id);
                stmtInsertNewOrder.setInt(i*3 +3, w_id);
            }
            int result = stmtInsertNewOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("new order not inserted");
            }
            //response.setInsno(result);
        }
    }

    private void insertOpenOrder(Connection conn, int w_id, int d_id, int[] c_ids, int[] o_ol_cnts, int[] o_all_locals, int o_id, SQLStmt stmtInsertOOrderMT) throws SQLException {
        try (PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderMT)) {
            // insert an oorder for each customer
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtInsertOOrder.setInt(i*7 +1, o_id + i);
                stmtInsertOOrder.setInt(i*7 +2, d_id);
                stmtInsertOOrder.setInt(i*7 +3, w_id);
                stmtInsertOOrder.setInt(i*7 +4, c_ids[i]);
                stmtInsertOOrder.setTimestamp(i*7 +5, new Timestamp(System.currentTimeMillis()));
                stmtInsertOOrder.setInt(i*7 +6, o_ol_cnts[i]);
                stmtInsertOOrder.setInt(i*7 +7, o_all_locals[i]);
            }
            int result = stmtInsertOOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("open order not inserted");
            }
            //response.setInsoo(result);
        }
    }

    private void updateDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL)) {
            stmtUpdateDist.setInt(1, MERGE_SIZE);
            stmtUpdateDist.setInt(2, w_id);
            stmtUpdateDist.setInt(3, d_id);
            int result = stmtUpdateDist.executeUpdate();
            if (result == 0) {
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
            }
            //response.setUpddist(result);
        }
    }

    private int getDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL)) {
            stmtGetDist.setInt(1, w_id);
            stmtGetDist.setInt(2, d_id);
            try (ResultSet rs = stmtGetDist.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                }
                //response.setNextoid(rs.getInt("D_NEXT_O_ID"));
                //response.setDtax(rs.getFloat("D_TAX"));
                return rs.getInt("D_NEXT_O_ID");
            }
        }
    }

    private void getWarehouse(Connection conn, int w_id) throws SQLException {
        try (PreparedStatement stmtGetWhse = this.getPreparedStatement(conn, stmtGetWhseSQL)) {
            stmtGetWhse.setInt(1, w_id);
            try (ResultSet rs = stmtGetWhse.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }
                //response.setWtax(rs.getFloat("W_TAX"));
            }
        }
    }

    private void getCustomer(Connection conn, int w_id, int d_id, int[] c_ids, SQLStmt stmtGetCustMT) throws SQLException {
        try (PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustMT)) {
            stmtGetCust.setInt(1, w_id);
            stmtGetCust.setInt(2, d_id);
            // set each c_ID
            for (int i = 3; i < MERGE_SIZE + 3; i++) {
               	stmtGetCust.setInt(i, c_ids[i-3]);
            }
            try (ResultSet rs = stmtGetCust.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("getCustomer result not found!");
                }
                // rs.next() return true and rs cursor move to the first row
                // response.setDiscount(rs.getFloat("C_DISCOUNT"));
                // response.setLast(rs.getString("C_LAST"));
                // response.setCredit(rs.getString("C_CREDIT"));
            }
        }
    }

    private List<Integer> arrayToList(int[] ints) {
        List<Integer> intList = new ArrayList<Integer>(ints.length);
        for (int i : ints) {
            intList.add(Integer.valueOf(i));
        }
        return intList;
    }
    
}

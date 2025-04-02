package com.grpc.examples.spree;

import com.grpc.examples.Procedure;
import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.SpreeNewOrderReply;
import com.grpc.examples.SpreeNewOrderRequest;

import java.util.Arrays;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpreeNewOrderMT extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(SpreeNewOrderMT.class);

    private SpreeNewOrderReply.Builder response = SpreeNewOrderReply.newBuilder();

    private int MERGE_SIZE;

    public SpreeNewOrderMT(int merge) {
        MERGE_SIZE = merge;
    }

    public SpreeNewOrderReply spreeNewOrderMergeTransaction(SpreeNewOrderRequest[] requests, Connection conn) throws SQLException {

        String state = requests[0].getState();
        String currency = requests[0].getCurrency();

        int[] user_ids = new int[MERGE_SIZE];
        String[] emails = new String[MERGE_SIZE];
        String[] numbers = new String[MERGE_SIZE];
        int[] created_by_ids = new int[MERGE_SIZE];
        int[] store_ids = new int[MERGE_SIZE];

        for (int i = 0; i < MERGE_SIZE; i++) {
            int user_id = requests[i].getUserId();
            String email = requests[i].getEmail();
            String number = requests[i].getNumber();
            int created_by_id = requests[i].getCreatedById();
            int store_id = requests[i].getStoreId();
            // check number has no duplicate
            while(Arrays.stream(numbers).anyMatch(number::equals)){
                number = "R" + SpreeUtil.randomNStr(31);
            }
			user_ids[i] = user_id;
            emails[i] = email;
            numbers[i] = number;
            created_by_ids[i] = created_by_id;
            store_ids[i] = store_id;
		}

        String stmtCheckNumMTSQL = "SELECT 1 AS one FROM spree_orders " +
                                "WHERE spree_orders.number IN (";

        String stmtCheckBinNumMTSQL = "SELECT 1 AS one FROM spree_orders " +
                                "WHERE spree_orders.number IN (";

        String stmtInsertOrderMTSQL = "INSERT INTO spree_orders" +
            " (number, state, user_id, email, created_at, updated_at, currency, created_by_id, store_id) " +
            " VALUES ";
		
        // concatenate SQL strings with certain number of specific '?'
        for (int i = 0; i < MERGE_SIZE - 1; i++) {
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

            // unwrap inner loop for each customer
            for (int i = 0; i < MERGE_SIZE; i++) {
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
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtInsertOrder.setString(i*9+1, numbers[i]);
                stmtInsertOrder.setString(i*9+2, state);
                stmtInsertOrder.setInt(i*9+3, user_ids[i]);
                stmtInsertOrder.setString(i*9+4, emails[i]);
                stmtInsertOrder.setTimestamp(i*9+5, sysdate);
                stmtInsertOrder.setTimestamp(i*9+6, sysdate);
                stmtInsertOrder.setString(i*9+7, currency);
                stmtInsertOrder.setInt(i*9+8, created_by_ids[i]);
                stmtInsertOrder.setInt(i*9+9, store_ids[i]);
            }
            int result = stmtInsertOrder.executeUpdate();
            if (result == 0) {
                LOG.error("new orders not inserted");
            }
        }
        response.setValid(true);
        response.setInserted(true);
        return response.build();

    }
    
}

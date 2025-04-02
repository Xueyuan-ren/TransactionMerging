package com.grpc.examples.spree;

import com.grpc.examples.Procedure;
import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.SpreeNewOrderReply;
import com.grpc.examples.SpreeNewOrderRequest;

import java.util.Arrays;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpreeNewOrder extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(SpreeNewOrder.class);

    private SpreeNewOrderReply.Builder response = SpreeNewOrderReply.newBuilder();

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


    public SpreeNewOrderReply spreeNewOrderTransaction(SpreeNewOrderRequest request, Connection conn) throws SQLException {

        String state = request.getState();
        String currency = request.getCurrency();

        int user_id = request.getUserId();
        String email = request.getEmail();
        String number = request.getNumber();
        int created_by_id = request.getCreatedById();
        int store_id = request.getStoreId();

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

        response.setValid(true);
        response.setInserted(true);
        return response.build();

    }
    
}

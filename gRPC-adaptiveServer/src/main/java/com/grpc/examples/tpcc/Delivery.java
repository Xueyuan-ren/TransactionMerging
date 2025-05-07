package com.grpc.examples.tpcc;

import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.DeliveryReply;
import com.grpc.examples.DeliveryRequest;
import com.grpc.examples.Procedure;

import java.math.BigDecimal;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Delivery extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Delivery.class);

    private DeliveryReply.Builder response = DeliveryReply.newBuilder();

    public SQLStmt delivGetOrderIdSQL = new SQLStmt(
            "SELECT NO_O_ID FROM " + TPCCConstants.TABLENAME_NEWORDER +
            " WHERE NO_D_ID = ? " +
            "   AND NO_W_ID = ? " +
            " ORDER BY NO_O_ID ASC " +
            " LIMIT 1 FOR UPDATE");

    public SQLStmt delivDeleteNewOrderSQL = new SQLStmt(
            "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER +
            " WHERE NO_O_ID = ? " +
            "   AND NO_D_ID = ?" +
            "   AND NO_W_ID = ?");

    public SQLStmt delivGetCustIdSQL = new SQLStmt(
            "SELECT O_C_ID FROM " + TPCCConstants.TABLENAME_OPENORDER +
            " WHERE O_ID = ? " +
            "   AND O_D_ID = ? " +
            "   AND O_W_ID = ?");

    public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_OPENORDER +
            "   SET O_CARRIER_ID = ? " +
            " WHERE O_ID = ? " +
            "   AND O_D_ID = ?" +
            "   AND O_W_ID = ?");

    public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE +
            "   SET OL_DELIVERY_D = ? " +
            " WHERE OL_O_ID = ? " +
            "   AND OL_D_ID = ? " +
            "   AND OL_W_ID = ? ");

    public SQLStmt delivSumOrderAmountSQL = new SQLStmt(
            "SELECT SUM(OL_AMOUNT) AS OL_TOTAL " +
            "  FROM " + TPCCConstants.TABLENAME_ORDERLINE +
            " WHERE OL_O_ID = ? " +
            "   AND OL_D_ID = ? " +
            "   AND OL_W_ID = ?");

    public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = C_BALANCE + ?," +
            "       C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ? ");


    public DeliveryReply deliveryTransaction(DeliveryRequest request, Connection conn) throws SQLException {

        int w_id = request.getTerminalWarehouseID();
        int o_carrier_id = request.getCarrierID();
        int terminalDistrictUpperID = request.getTerminalDistrictUpperID();

        int d_id;

        int[] orderIDs = new int[10];

        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {
            Integer no_o_id = getOrderId(conn, w_id, d_id);

            if (no_o_id == null) {
                continue;
            }

            orderIDs[d_id - 1] = no_o_id;

            deleteOrder(conn, w_id, d_id, no_o_id);

            int customerId = getCustomerId(conn, w_id, d_id, no_o_id);

            updateCarrierId(conn, w_id, o_carrier_id, d_id, no_o_id);

            updateDeliveryDate(conn, w_id, d_id, no_o_id);

            float orderLineTotal = getOrderLineTotal(conn, w_id, d_id, no_o_id);

            updateBalanceAndDelivery(conn, w_id, d_id, customerId, orderLineTotal);
        }

        return response.build();

    }

    private Integer getOrderId(Connection conn, int w_id, int d_id) throws SQLException {

        try (PreparedStatement delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL)) {
            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);

            try (ResultSet rs = delivGetOrderId.executeQuery()) {

                if (!rs.next()) {
                    // This district has no new orders.  This can happen but should be rare

                    LOG.warn(String.format("District has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));

                    return null;
                }

                return rs.getInt("NO_O_ID");

            }
        }
    }

    private void deleteOrder(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        try (PreparedStatement delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderSQL)) {
            delivDeleteNewOrder.setInt(1, no_o_id);
            delivDeleteNewOrder.setInt(2, d_id);
            delivDeleteNewOrder.setInt(3, w_id);

            int result = delivDeleteNewOrder.executeUpdate();

            if (result != 1) {
                // This code used to run in a loop in an attempt to make this work
                // with MySQL's default weird consistency level. We just always run
                // this as SERIALIZABLE instead. I don't *think* that fixing this one
                // error makes this work with MySQL's default consistency.
                // Careful auditing would be required.
                String msg = String.format("NewOrder delete failed. Not running with SERIALIZABLE isolation? [w_id=%d, d_id=%d, no_o_id=%d]", w_id, d_id, no_o_id);
                throw new UserAbortException(msg);
            }

            response.addDelno(result);
        }
    }

    private int getCustomerId(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {

        try (PreparedStatement delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL)) {
            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);

            try (ResultSet rs = delivGetCustId.executeQuery()) {

                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }

                return rs.getInt("O_C_ID");
            }
        }
    }

    private void updateCarrierId(Connection conn, int w_id, int o_carrier_id, int d_id, int no_o_id) throws SQLException {
        try (PreparedStatement delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL)) {
            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setInt(2, no_o_id);
            delivUpdateCarrierId.setInt(3, d_id);
            delivUpdateCarrierId.setInt(4, w_id);

            int result = delivUpdateCarrierId.executeUpdate();

            if (result != 1) {
                String msg = String.format("Failed to update ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                throw new RuntimeException(msg);
            }

            response.addUpdcar(result);
        }
    }

    private void updateDeliveryDate(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        try (PreparedStatement delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL)) {
            delivUpdateDeliveryDate.setTimestamp(1, timestamp);
            delivUpdateDeliveryDate.setInt(2, no_o_id);
            delivUpdateDeliveryDate.setInt(3, d_id);
            delivUpdateDeliveryDate.setInt(4, w_id);

            int result = delivUpdateDeliveryDate.executeUpdate();

            if (result == 0) {
                String msg = String.format("Failed to update ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                throw new RuntimeException(msg);
            }

            response.addUpddd(result);
        }
    }

    private float getOrderLineTotal(Connection conn, int w_id, int d_id, int no_o_id) throws SQLException {
        try (PreparedStatement delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL)) {
            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);

            try (ResultSet rs = delivSumOrderAmount.executeQuery()) {
                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }

                return rs.getFloat("OL_TOTAL");
            }
        }
    }

    private void updateBalanceAndDelivery(Connection conn, int w_id, int d_id, int c_id, float orderLineTotal) throws SQLException {

        try (PreparedStatement delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL)) {
            delivUpdateCustBalDelivCnt.setBigDecimal(1, BigDecimal.valueOf(orderLineTotal));
            delivUpdateCustBalDelivCnt.setInt(2, w_id);
            delivUpdateCustBalDelivCnt.setInt(3, d_id);
            delivUpdateCustBalDelivCnt.setInt(4, c_id);

            int result = delivUpdateCustBalDelivCnt.executeUpdate();

            if (result == 0) {
                String msg = String.format("Failed to update CUSTOMER record [W_ID=%d, D_ID=%d, C_ID=%d]", w_id, d_id, c_id);
                throw new RuntimeException(msg);
            }

            response.addUpdbd(result);
        }
    }
}

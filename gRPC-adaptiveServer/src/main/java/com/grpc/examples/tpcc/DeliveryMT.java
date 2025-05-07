package com.grpc.examples.tpcc;

import com.grpc.examples.Procedure;
import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.DeliveryReply;
import com.grpc.examples.DeliveryRequest;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeliveryMT extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(Delivery.class);

    private DeliveryReply.Builder response = DeliveryReply.newBuilder();

    private int MERGE_SIZE;

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

    public DeliveryMT(int merge) {
        MERGE_SIZE = merge;
    }

    public DeliveryReply deliveryInternalMergeTransaction(DeliveryRequest request, Connection conn) throws SQLException {

        int w_id = request.getTerminalWarehouseID();
        int o_carrier_id = request.getCarrierID();
        int terminalDistrictUpperID = request.getTerminalDistrictUpperID();

        int d_id;

        //int[] orderIDs = new int[10];
        ArrayList<Integer> d_ids = new ArrayList<>();
        ArrayList<Integer> neworder_ids = new ArrayList<>();
        Map<Integer, Integer> did_cid = new HashMap<>();
        Map<Integer, Float> did_oltotal = new HashMap<>();
        
        // terminalDistrictUpperID is less than or equal to 10 (configDistPerWhse)
        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {
            Integer no_o_id = getOrderId(conn, w_id, d_id);

            if (no_o_id == null) {
                continue;
            }

            //orderIDs[d_id - 1] = no_o_id;
            d_ids.add(d_id);
            neworder_ids.add(no_o_id);
        }

        if (neworder_ids.size() == 0) {
            // no new order for this warehouse districts
            String msg = String.format("No NewOrder exists for this warehouse and selected districts. [w_id=%d]", w_id);
            throw new UserAbortException(msg);
        }

        //deleteOrder(conn, w_id, d_id, no_o_id);
        deleteOrders(conn, w_id, d_ids, neworder_ids);

        //int customerId = getCustomerId(conn, w_id, d_id, no_o_id);
        did_cid = getCustomerIds(conn, w_id, d_ids, neworder_ids);

        //updateCarrierId(conn, w_id, o_carrier_id, d_id, no_o_id);
        updateCarrierIds(conn, w_id, o_carrier_id, d_ids, neworder_ids);

        //updateDeliveryDate(conn, w_id, d_id, no_o_id);
        updateDeliveryDates(conn, w_id, d_ids, neworder_ids);

        //float orderLineTotal = getOrderLineTotal(conn, w_id, d_id, no_o_id);
        did_oltotal = getOrderLineTotals(conn, w_id, d_ids, neworder_ids);

        //updateBalanceAndDelivery(conn, w_id, d_id, customerId, orderLineTotal);
        updateBalanceAndDeliverys(conn, w_id, d_ids, did_cid, did_oltotal);
        
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

    private void deleteOrders(Connection conn, int w_id, ArrayList<Integer> d_ids, ArrayList<Integer> no_o_ids) throws SQLException {
        String delivDeleteNewOrderMTSQL =
            "DELETE FROM " + TPCCConstants.TABLENAME_NEWORDER +
            " WHERE (NO_W_ID, NO_D_ID, NO_O_ID) IN (";
        // there may be 0 in no_o_id array indicating no new order or district not included, use another ArrayList
        for (int i = 0; i < no_o_ids.size() - 1; i++) {
            delivDeleteNewOrderMTSQL += "(?, ?, ?),";
        }
        delivDeleteNewOrderMTSQL += "(?, ?, ?))";
        SQLStmt delivDeleteNewOrderMT = new SQLStmt(delivDeleteNewOrderMTSQL);

        try (PreparedStatement delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderMT)) {
            
            for (int i = 0; i < no_o_ids.size(); i++) {
                delivDeleteNewOrder.setInt(i*3 + 1, w_id);
                delivDeleteNewOrder.setInt(i*3 + 2, d_ids.get(i));
                delivDeleteNewOrder.setInt(i*3 + 3, no_o_ids.get(i));
            }

            int result = delivDeleteNewOrder.executeUpdate();

            if (result != no_o_ids.size()) {
                String msg = String.format("NewOrder delete failed. Not running with SERIALIZABLE isolation? [w_id=%d, d_ids=%s, no_o_ids=%s", w_id, d_ids.toString(), no_o_ids.toString());
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

    private Map<Integer, Integer> getCustomerIds(Connection conn, int w_id, ArrayList<Integer> d_ids, ArrayList<Integer> no_o_ids) throws SQLException {
        Map<Integer, Integer> did_cid = new HashMap<>();
        String delivGetCustIdMTSQL = 
            "SELECT O_D_ID, O_C_ID FROM " + TPCCConstants.TABLENAME_OPENORDER +
            " WHERE (O_W_ID, O_D_ID, O_ID) IN (";

        for (int i = 0; i < no_o_ids.size() - 1; i++) {
            delivGetCustIdMTSQL += "(?, ?, ?),";
        }
        delivGetCustIdMTSQL += "(?, ?, ?))";
        SQLStmt delivGetCustIdMT = new SQLStmt(delivGetCustIdMTSQL);

        try (PreparedStatement delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdMT)) {
            for (int i = 0; i < no_o_ids.size(); i++) {
                delivGetCustId.setInt(i*3 + 1, w_id);
                delivGetCustId.setInt(i*3 + 2, d_ids.get(i));
                delivGetCustId.setInt(i*3 + 3, no_o_ids.get(i));
            }

            try (ResultSet rs = delivGetCustId.executeQuery()) {
                while (rs.next()) {
                    did_cid.put(rs.getInt("O_D_ID"), rs.getInt("O_C_ID"));
                }
            }

            if (did_cid.size() != no_o_ids.size()) {
                String msg = String.format("Failed to retrieve all ORDER record [W_ID=%d, D_IDs=%s, O_IDs=%s", w_id, d_ids.toString(), no_o_ids.toString());
                throw new RuntimeException(msg);
            }
        }
        return did_cid;
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

    private void updateCarrierIds(Connection conn, int w_id, int o_carrier_id, ArrayList<Integer> d_ids, ArrayList<Integer> no_o_ids) throws SQLException {
        String delivUpdateCarrierIdMTSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_OPENORDER +
            " SET O_CARRIER_ID = ? " +
            "WHERE (O_W_ID, O_D_ID, O_ID) IN (";

        for (int i = 0; i < no_o_ids.size() - 1; i++) {
            delivUpdateCarrierIdMTSQL += "(?, ?, ?),";
        }
        delivUpdateCarrierIdMTSQL += "(?, ?, ?))";
        SQLStmt delivUpdateCarrierIdMT = new SQLStmt(delivUpdateCarrierIdMTSQL);

        try (PreparedStatement delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdMT)) {
            delivUpdateCarrierId.setInt(1, o_carrier_id);
            for (int i = 0; i < no_o_ids.size(); i++) {   
                delivUpdateCarrierId.setInt(i*3 + 2, w_id);
                delivUpdateCarrierId.setInt(i*3 + 3, d_ids.get(i));
                delivUpdateCarrierId.setInt(i*3 + 4, no_o_ids.get(i));
            }

            int result = delivUpdateCarrierId.executeUpdate();

            if (result != no_o_ids.size()) {
                String msg = String.format("Failed to update all ORDER record [W_ID=%d, D_IDs=%s, O_IDs=%s", w_id, d_ids.toString(), no_o_ids.toString());
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

    private void updateDeliveryDates(Connection conn, int w_id, ArrayList<Integer> d_ids, ArrayList<Integer> no_o_ids) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        String delivUpdateDeliveryDateMTSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_ORDERLINE +
            " SET OL_DELIVERY_D = ? " +
            "WHERE (OL_W_ID, OL_D_ID, OL_O_ID) IN (";

        for (int i = 0; i < no_o_ids.size() - 1; i++) {
            delivUpdateDeliveryDateMTSQL += "(?, ?, ?),";
        }
        delivUpdateDeliveryDateMTSQL += "(?, ?, ?))";
        SQLStmt delivUpdateDeliveryDateMT = new SQLStmt(delivUpdateDeliveryDateMTSQL);

        try (PreparedStatement delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateMT)) {
            delivUpdateDeliveryDate.setTimestamp(1, timestamp);
            for (int i = 0; i < no_o_ids.size(); i++) {   
                delivUpdateDeliveryDate.setInt(i*3 + 2, w_id);
                delivUpdateDeliveryDate.setInt(i*3 + 3, d_ids.get(i));
                delivUpdateDeliveryDate.setInt(i*3 + 4, no_o_ids.get(i));
            }

            int result = delivUpdateDeliveryDate.executeUpdate();
            // bug: cannot justify if the returned result is consistent with record count in order_line table
            // each order may have 5-15 order_line records
            // fix: may need to retrive the record count first(select count*) and then compare it with result
            if (result == 0) {
                String msg = String.format("Failed to update ORDER_LINE records [W_ID=%d, D_IDs=%s, O_IDs=%s", w_id, d_ids.toString(), no_o_ids.toString());
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

    private Map<Integer, Float> getOrderLineTotals(Connection conn, int w_id, ArrayList<Integer> d_ids, ArrayList<Integer> no_o_ids) throws SQLException {
        Map<Integer, Float> did_oltotal = new HashMap<>();
        String delivSumOrderAmountMTSQL = 
            "SELECT OL_D_ID, SUM(OL_AMOUNT) AS OL_TOTAL " +
            "FROM " + TPCCConstants.TABLENAME_ORDERLINE +
            " WHERE (OL_W_ID, OL_D_ID, OL_O_ID) IN (";

        for (int i = 0; i < no_o_ids.size() - 1; i++) {
            delivSumOrderAmountMTSQL += "(?, ?, ?),";
        }
        delivSumOrderAmountMTSQL += "(?, ?, ?)) GROUP BY OL_W_ID, OL_D_ID, OL_O_ID";
        SQLStmt delivSumOrderAmountMT = new SQLStmt(delivSumOrderAmountMTSQL);

        try (PreparedStatement delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountMT)) {
            for (int i = 0; i < no_o_ids.size(); i++) {
                delivSumOrderAmount.setInt(i*3 + 1, w_id);
                delivSumOrderAmount.setInt(i*3 + 2, d_ids.get(i));
                delivSumOrderAmount.setInt(i*3 + 3, no_o_ids.get(i));
            }

            try (ResultSet rs = delivSumOrderAmount.executeQuery()) {
                while (rs.next()) {
                    did_oltotal.put(rs.getInt("OL_D_ID"), rs.getFloat("OL_TOTAL"));
                }
            }

            if (did_oltotal.size() != no_o_ids.size()) {
                String msg = String.format("Failed to retrieve all ORDER_LINE record [W_ID=%d, D_IDs=%s, O_IDs=%s", w_id, d_ids.toString(), no_o_ids.toString());
                throw new RuntimeException(msg);
            }
        }
        return did_oltotal;
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

    private void updateBalanceAndDeliverys(Connection conn, int w_id, ArrayList<Integer> d_ids,
                                Map<Integer, Integer> did_cid, Map<Integer, Float> did_oltotal) throws SQLException {
        String delivUpdateCustBalDelivCntMTSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            " SET C_BALANCE = CASE ";
            // "C_DELIVERY_CNT = C_DELIVERY_CNT + 1 " +
            // "WHERE C_W_ID = ? " +
            // "AND C_D_ID = ? " +
            // "AND C_ID = ?";

        int cSize = did_cid.size();
        for (int i = 0; i < cSize; i++) {
            delivUpdateCustBalDelivCntMTSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        delivUpdateCustBalDelivCntMTSQL += " ELSE C_BALANCE END, C_DELIVERY_CNT = C_DELIVERY_CNT + 1 WHERE (C_W_ID, C_D_ID, C_ID) IN (";
        for (int i = 0; i < cSize - 1; i++) {
            delivUpdateCustBalDelivCntMTSQL += "(?,?,?),";
        }
        delivUpdateCustBalDelivCntMTSQL += "(?,?,?))";
        SQLStmt delivUpdateCustBalDelivCntMT = new SQLStmt(delivUpdateCustBalDelivCntMTSQL);

        try (PreparedStatement delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntMT)) {
            for (int i = 0; i < cSize; i++) {
                int d_id = d_ids.get(i);
                delivUpdateCustBalDelivCnt.setInt(i * 4 + 1, w_id);
                delivUpdateCustBalDelivCnt.setInt(i * 4 + 2, d_id);
                delivUpdateCustBalDelivCnt.setInt(i * 4 + 3, did_cid.get(d_id));
                delivUpdateCustBalDelivCnt.setBigDecimal(i * 4 + 4, BigDecimal.valueOf(did_oltotal.get(d_id)));

                delivUpdateCustBalDelivCnt.setInt(i * 3 + cSize*4 + 1, w_id);
                delivUpdateCustBalDelivCnt.setInt(i * 3 + cSize*4 + 2, d_id);
                delivUpdateCustBalDelivCnt.setInt(i * 3 + cSize*4 + 3, did_cid.get(d_id));
            }

            int result = delivUpdateCustBalDelivCnt.executeUpdate();

            if (result != cSize) {
                String msg = String.format("Failed to update all CUSTOMER record [W_ID=%d, D_IDs=%s, C_ID=%s", w_id, d_ids.toString(), did_cid.values().toString());
                throw new RuntimeException(msg);
            }

            response.addUpdbd(result);
        }
    }
}

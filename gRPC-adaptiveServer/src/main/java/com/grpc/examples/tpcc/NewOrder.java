package com.grpc.examples.tpcc;

import com.grpc.examples.Procedure;
import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.tpcc.pojo.Stock;
import com.grpc.examples.NewOrderRequest;
import com.grpc.examples.NewOrderReply;

import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewOrder extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrder.class);

    private NewOrderReply.Builder response = NewOrderReply.newBuilder();

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
            "SELECT C_DISCOUNT, C_LAST, C_CREDIT" +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
            "SELECT W_TAX " +
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?");

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID, D_TAX " +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE");

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_NEWORDER +
            " (NO_O_ID, NO_D_ID, NO_W_ID) " +
            " VALUES ( ?, ?, ?)");

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            "   SET D_NEXT_O_ID = D_NEXT_O_ID + 1 " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_OPENORDER +
            " (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL)" +
            " VALUES (?, ?, ?, ?, ?, ?, ?)");

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
            "SELECT I_PRICE, I_NAME , I_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_ITEM +
            " WHERE I_ID = ?");

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
            "SELECT S_QUANTITY, S_DATA, S_DIST_01, S_DIST_02, S_DIST_03, S_DIST_04, S_DIST_05, " +
            "       S_DIST_06, S_DIST_07, S_DIST_08, S_DIST_09, S_DIST_10" +
            "  FROM " + TPCCConstants.TABLENAME_STOCK +
            " WHERE S_I_ID = ? " +
            "   AND S_W_ID = ? FOR UPDATE");

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_STOCK +
            "   SET S_QUANTITY = ? , " +
            "       S_YTD = S_YTD + ?, " +
            "       S_ORDER_CNT = S_ORDER_CNT + 1, " +
            "       S_REMOTE_CNT = S_REMOTE_CNT + ? " +
            " WHERE S_I_ID = ? " +
            "   AND S_W_ID = ?");

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_ORDERLINE +
            " (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO) " +
            " VALUES (?,?,?,?,?,?,?,?,?)");


    public NewOrderReply newOrderTransaction(NewOrderRequest request, Connection conn) throws SQLException {

        int w_id = request.getTerminalWarehouseID();
        int d_id = request.getDistrictID();
        int c_id = request.getCustomerID();
        int o_ol_cnt = request.getNumItems();
        int o_all_local = request.getAllLocal();
        
        int[] itemIDs = new int[o_ol_cnt];
        int[] supplierWarehouseIDs = new int[o_ol_cnt];
        int[] orderQuantities = new int[o_ol_cnt];
        itemIDs = request.getItemIDsList().stream().mapToInt(Integer::intValue).toArray();
        supplierWarehouseIDs = request.getSupplierWarehouseIDsList().stream().mapToInt(Integer::intValue).toArray();
        orderQuantities = request.getOrderQuantitiesList().stream().mapToInt(Integer::intValue).toArray();
		
        getCustomer(conn, w_id, d_id, c_id);

        getWarehouse(conn, w_id);

        int d_next_o_id = getDistrict(conn, w_id, d_id);

        updateDistrict(conn, w_id, d_id);

        insertOpenOrder(conn, w_id, d_id, c_id, o_ol_cnt, o_all_local, d_next_o_id);

        insertNewOrder(conn, w_id, d_id, d_next_o_id);

        try (PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL);
             PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQL)) {

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                int ol_i_id = itemIDs[ol_number - 1];
                int ol_quantity = orderQuantities[ol_number - 1];

                // this may occasionally error and that's ok!
                float i_price = getItemPrice(conn, ol_i_id);

                float ol_amount = ol_quantity * i_price;

                Stock s = getStock(conn, ol_supply_w_id, ol_i_id, ol_quantity);

                String ol_dist_info = getDistInfo(d_id, s);

                stmtInsertOrderLine.setInt(1, d_next_o_id);
                stmtInsertOrderLine.setInt(2, d_id);
                stmtInsertOrderLine.setInt(3, w_id);
                stmtInsertOrderLine.setInt(4, ol_number);
                stmtInsertOrderLine.setInt(5, ol_i_id);
                stmtInsertOrderLine.setInt(6, ol_supply_w_id);
                stmtInsertOrderLine.setInt(7, ol_quantity);
                stmtInsertOrderLine.setDouble(8, ol_amount);
                stmtInsertOrderLine.setString(9, ol_dist_info);
                stmtInsertOrderLine.addBatch();

                int s_remote_cnt_increment;

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                stmtUpdateStock.setInt(1, s.s_quantity);
                stmtUpdateStock.setInt(2, ol_quantity);
                stmtUpdateStock.setInt(3, s_remote_cnt_increment);
                stmtUpdateStock.setInt(4, ol_i_id);
                stmtUpdateStock.setInt(5, ol_supply_w_id);
                stmtUpdateStock.addBatch();

            }

            int[] rsinsol = stmtInsertOrderLine.executeBatch();
            //response.addAllInsol(arrayToList(rsinsol));
            stmtInsertOrderLine.clearBatch();

            int[] rsupdst = stmtUpdateStock.executeBatch();
            //response.addAllUpdst(arrayToList(rsupdst));
            stmtUpdateStock.clearBatch();

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

    private Stock getStock(Connection conn, int ol_supply_w_id, int ol_i_id, int ol_quantity) throws SQLException {
        try (PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL)) {
            stmtGetStock.setInt(1, ol_i_id);
            stmtGetStock.setInt(2, ol_supply_w_id);
            try (ResultSet rs = stmtGetStock.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " not found!");
                }
                Stock s = new Stock();
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

                if (s.s_quantity - ol_quantity >= 10) {
                    s.s_quantity -= ol_quantity;
                } else {
                    s.s_quantity += -ol_quantity + 91;
                }

                return s;
            }
        }
    }

    private float getItemPrice(Connection conn, int ol_i_id) throws SQLException {
        try (PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL)) {
            stmtGetItem.setInt(1, ol_i_id);
            try (ResultSet rs = stmtGetItem.executeQuery()) {
                if (!rs.next()) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException("EXPECTED new order rollback: I_ID=" + ol_i_id + " not found!");
                }

                return rs.getFloat("I_PRICE");
            }
        }
    }

    private void insertNewOrder(Connection conn, int w_id, int d_id, int o_id) throws SQLException {
        try (PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderSQL);) {
            stmtInsertNewOrder.setInt(1, o_id);
            stmtInsertNewOrder.setInt(2, d_id);
            stmtInsertNewOrder.setInt(3, w_id);
            int result = stmtInsertNewOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("new order not inserted");
            }
            //response.setInsno(result);
        }
    }

    private void insertOpenOrder(Connection conn, int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int o_id) throws SQLException {
        try (PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderSQL);) {
            stmtInsertOOrder.setInt(1, o_id);
            stmtInsertOOrder.setInt(2, d_id);
            stmtInsertOOrder.setInt(3, w_id);
            stmtInsertOOrder.setInt(4, c_id);
            stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmtInsertOOrder.setInt(6, o_ol_cnt);
            stmtInsertOOrder.setInt(7, o_all_local);

            int result = stmtInsertOOrder.executeUpdate();

            if (result == 0) {
                LOG.warn("open order not inserted");
            }
            //response.setInsoo(result);
        }
    }

    private void updateDistrict(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL)) {
            stmtUpdateDist.setInt(1, w_id);
            stmtUpdateDist.setInt(2, d_id);
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

    private void getCustomer(Connection conn, int w_id, int d_id, int c_id) throws SQLException {
        try (PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustSQL)) {
            stmtGetCust.setInt(1, w_id);
            stmtGetCust.setInt(2, d_id);
            stmtGetCust.setInt(3, c_id);
            try (ResultSet rs = stmtGetCust.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }
                // rs.next() return true and rs cursor move to the first row
                //response.setDiscount(rs.getFloat("C_DISCOUNT"));
                //response.setLast(rs.getString("C_LAST"));
                //response.setCredit(rs.getString("C_CREDIT"));
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

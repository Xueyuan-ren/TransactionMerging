package com.grpc.examples.tpcc;


import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.Procedure;
import com.grpc.examples.StockLevelReply;
import com.grpc.examples.StockLevelRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public class StockLevel extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(StockLevel.class);

    private StockLevelReply.Builder response = StockLevelReply.newBuilder();

    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
            "SELECT D_NEXT_O_ID " +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt stockGetCountStockSQL = new SQLStmt(
            "SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT " +
            " FROM " + TPCCConstants.TABLENAME_ORDERLINE + ", " + TPCCConstants.TABLENAME_STOCK +
            " WHERE OL_W_ID = ?" +
            " AND OL_D_ID = ?" +
            " AND OL_O_ID < ?" +
            " AND OL_O_ID >= ?" +
            " AND S_W_ID = ?" +
            " AND S_I_ID = OL_I_ID" +
            " AND S_QUANTITY < ?");
    
    
    public StockLevelReply stockLevelTransaction(StockLevelRequest request, Connection conn) throws SQLException {

        int w_id = request.getTerminalWarehouseID();
        int threshold = request.getThreshold();
        int d_id = request.getDistrictID();

        int o_id = getOrderId(conn, w_id, d_id);

        int stock_count = getStockCount(conn, w_id, threshold, d_id, o_id);

        return response.build();

    }

    private int getOrderId(Connection conn, int w_id, int d_id) throws SQLException {
        try (PreparedStatement stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL)) {
            stockGetDistOrderId.setInt(1, w_id);
            stockGetDistOrderId.setInt(2, d_id);

            try (ResultSet rs = stockGetDistOrderId.executeQuery()) {

                if (!rs.next()) {
                    throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
                }

                response.setOId(rs.getInt("D_NEXT_O_ID"));
                return rs.getInt("D_NEXT_O_ID");
            }
        }

    }

    private int getStockCount(Connection conn, int w_id, int threshold, int d_id, int o_id) throws SQLException {
        try (PreparedStatement stockGetCountStock = this.getPreparedStatement(conn, stockGetCountStockSQL)) {
            stockGetCountStock.setInt(1, w_id);
            stockGetCountStock.setInt(2, d_id);
            stockGetCountStock.setInt(3, o_id);
            stockGetCountStock.setInt(4, o_id - 20);
            stockGetCountStock.setInt(5, w_id);
            stockGetCountStock.setInt(6, threshold);

            try (ResultSet rs = stockGetCountStock.executeQuery()) {
                if (!rs.next()) {
                    String msg = String.format("Failed to get StockLevel result for COUNT query [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, o_id);

                    throw new RuntimeException(msg);
                }

                response.setStockCount(rs.getInt("STOCK_COUNT"));

                return rs.getInt("STOCK_COUNT");
            }
        }
    }
}

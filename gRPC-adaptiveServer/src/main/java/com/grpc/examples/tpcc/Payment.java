package com.grpc.examples.tpcc;

import com.grpc.examples.api.SQLStmt;
import com.grpc.examples.tpcc.pojo.Customer;
import com.grpc.examples.tpcc.pojo.District;
import com.grpc.examples.tpcc.pojo.Warehouse;
import com.grpc.examples.PaymentReply;
import com.grpc.examples.PaymentRequest;
import com.grpc.examples.Procedure;

import java.util.ArrayList;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Payment extends Procedure {
    
    private static final Logger LOG = LoggerFactory.getLogger(Payment.class);

    private final Random gen = new Random();
    private PaymentReply.Builder response = PaymentReply.newBuilder();

    public SQLStmt payUpdateWhseSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_WAREHOUSE +
            "   SET W_YTD = W_YTD + ? " +
            " WHERE W_ID = ? ");

    public SQLStmt payGetWhseSQL = new SQLStmt(
            "SELECT W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_WAREHOUSE +
            " WHERE W_ID = ?");

    public SQLStmt payUpdateDistSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_DISTRICT +
            "   SET D_YTD = D_YTD + ? " +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt payGetDistSQL = new SQLStmt(
            "SELECT D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_NAME" +
            "  FROM " + TPCCConstants.TABLENAME_DISTRICT +
            " WHERE D_W_ID = ? " +
            "   AND D_ID = ?");

    public SQLStmt payGetCustSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "       C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "       C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payGetCustCdataSQL = new SQLStmt(
            "SELECT C_DATA " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ?, " +
            "       C_DATA = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = ?, " +
            "       C_YTD_PAYMENT = ?, " +
            "       C_PAYMENT_CNT = ? " +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_ID = ?");

    public SQLStmt payInsertHistSQL = new SQLStmt(
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            " VALUES (?,?,?,?,?,?,?,?)");

    public SQLStmt customerByNameSQL = new SQLStmt(
            "SELECT C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "       C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "       C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "  FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE C_W_ID = ? " +
            "   AND C_D_ID = ? " +
            "   AND C_LAST = ? " +
            " ORDER BY C_FIRST");


    public PaymentReply paymentTransaction(PaymentRequest request, Connection conn) throws SQLException {

        int terminalWarehouseID = request.getTerminalWarehouseID();
        int districtID = request.getDistrictID();
        float paymentAmount = request.getPaymentAmount();
        int customerDistrictID = request.getCustomerDistrictID();
        int customerWarehouseID = request.getCustomerWarehouseID();

        updateWarehouse(conn, terminalWarehouseID, paymentAmount);

        Warehouse w = getWarehouse(conn, terminalWarehouseID);

        updateDistrict(conn, terminalWarehouseID, districtID, paymentAmount);

        District d = getDistrict(conn, terminalWarehouseID, districtID);

        Customer c = getCustomer(conn, gen, customerDistrictID, customerWarehouseID, paymentAmount);

        if (c.c_credit.equals("BC")) {
            // bad credit
            c.c_data = getCData(conn, terminalWarehouseID, districtID, customerDistrictID, customerWarehouseID, paymentAmount, c);

            updateBalanceCData(conn, customerDistrictID, customerWarehouseID, c);

        } else {
            // GoodCredit

            updateBalance(conn, customerDistrictID, customerWarehouseID, c);

        }

        insertHistory(conn, terminalWarehouseID, districtID, customerDistrictID, customerWarehouseID, paymentAmount, w.w_name, d.d_name, c);
        
        response.setCompleted(true);
        return response.build();

    }

    private void updateWarehouse(Connection conn, int w_id, float paymentAmount) throws SQLException {
        try (PreparedStatement payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL)) {
            payUpdateWhse.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
            payUpdateWhse.setInt(2, w_id);
            // MySQL reports deadlocks due to lock upgrades:
            // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
            int result = payUpdateWhse.executeUpdate();
            if (result == 0) {
                throw new RuntimeException("W_ID=" + w_id + " not found!");
            }
            //response.setUpdwh(result);
        }
    }

    private Warehouse getWarehouse(Connection conn, int w_id) throws SQLException {
        try (PreparedStatement payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL)) {
            payGetWhse.setInt(1, w_id);

            try (ResultSet rs = payGetWhse.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }

                Warehouse w = new Warehouse();
                w.w_street_1 = rs.getString("W_STREET_1");
                w.w_street_2 = rs.getString("W_STREET_2");
                w.w_city = rs.getString("W_CITY");
                w.w_state = rs.getString("W_STATE");
                w.w_zip = rs.getString("W_ZIP");
                w.w_name = rs.getString("W_NAME");

                // Winfo warehouse = Winfo.newBuilder()
                //     .setWstreet1(w.w_street_1)
                //     .setWstreet2(w.w_street_2)
                //     .setWcity(w.w_city)
                //     .setWstate(w.w_state)
                //     .setWzip(w.w_zip)
                //     .setWname(w.w_name)
                //     .build();
                // response.setWinfo(warehouse);

                return w;
            }
        }
    }

    private Customer getCustomer(Connection conn, Random gen, int customerDistrictID, int customerWarehouseID, float paymentAmount) throws SQLException {
        int y = TPCCUtil.randomNumber(1, 100, gen);

        Customer c;

        if (y <= 60) {
            // 60% lookups by last name
            c = getCustomerByName(customerWarehouseID, customerDistrictID, TPCCUtil.getNonUniformRandomLastNameForRun(gen), conn);
        } else {
            // 40% lookups by customer ID
            c = getCustomerById(customerWarehouseID, customerDistrictID, TPCCUtil.getCustomerID(gen), conn);
        }

        c.c_balance -= paymentAmount;
        c.c_ytd_payment += paymentAmount;
        c.c_payment_cnt += 1;

        // Cinfo customer = Cinfo.newBuilder()
        //     .setCfirst(c.c_first)
        //     .setCmiddle(c.c_middle)
        //     .setClast(c.c_last)
        //     .setCstreet1(c.c_street_1)
        //     .setCstreet2(c.c_street_2)
        //     .setCcity(c.c_city)
        //     .setCstate(c.c_state)
        //     .setCzip(c.c_zip)
        //     .setCphone(c.c_phone)
        //     .setCcredit(c.c_credit)
        //     .setCcreditLim(c.c_credit_lim)
        //     .setCdiscount(c.c_discount)
        //     .setCbalance(c.c_balance)
        //     .setCytdpayment(c.c_ytd_payment)
        //     .setCpaymentCnt(c.c_payment_cnt)
        //     .setCsince((c.c_since).toString())
        //     .build();
        // response.setCinfo(customer);

        return c;
    }

    private void updateDistrict(Connection conn, int w_id, int districtID, float paymentAmount) throws SQLException {
        try (PreparedStatement payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL)) {
            payUpdateDist.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
            payUpdateDist.setInt(2, w_id);
            payUpdateDist.setInt(3, districtID);

            int result = payUpdateDist.executeUpdate();

            if (result == 0) {
                throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
            }
            //response.setUpddist(result);
        }
    }

    private District getDistrict(Connection conn, int w_id, int districtID) throws SQLException {
        try (PreparedStatement payGetDist = this.getPreparedStatement(conn, payGetDistSQL)) {
            payGetDist.setInt(1, w_id);
            payGetDist.setInt(2, districtID);

            try (ResultSet rs = payGetDist.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
                }

                District d = new District();
                d.d_street_1 = rs.getString("D_STREET_1");
                d.d_street_2 = rs.getString("D_STREET_2");
                d.d_city = rs.getString("D_CITY");
                d.d_state = rs.getString("D_STATE");
                d.d_zip = rs.getString("D_ZIP");
                d.d_name = rs.getString("D_NAME");

                // Dinfo district = Dinfo.newBuilder()
                //     .setDstreet1(d.d_street_1)
                //     .setDstreet2(d.d_street_2)
                //     .setDcity(d.d_city)
                //     .setDstate(d.d_state)
                //     .setDzip(d.d_zip)
                //     .setDname(d.d_name)
                //     .build();
                // response.setDinfo(district);

                return d;
            }
        }
    }

    private String getCData(Connection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, Customer c) throws SQLException {

        try (PreparedStatement payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL)) {
            String c_data;
            payGetCustCdata.setInt(1, customerWarehouseID);
            payGetCustCdata.setInt(2, customerDistrictID);
            payGetCustCdata.setInt(3, c.c_id);
            try (ResultSet rs = payGetCustCdata.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
                }
                c_data = rs.getString("C_DATA");
            }

            c_data = c.c_id + " " + customerDistrictID + " " + customerWarehouseID + " " + districtID + " " + w_id + " " + paymentAmount + " | " + c_data;
            if (c_data.length() > 500) {
                c_data = c_data.substring(0, 500);
            }

            return c_data;
        }

    }

    private void updateBalanceCData(Connection conn, int customerDistrictID, int customerWarehouseID, Customer c) throws SQLException {
        try (PreparedStatement payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL)) {
            payUpdateCustBalCdata.setDouble(1, c.c_balance);
            payUpdateCustBalCdata.setDouble(2, c.c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c.c_data);
            payUpdateCustBalCdata.setInt(5, customerWarehouseID);
            payUpdateCustBalCdata.setInt(6, customerDistrictID);
            payUpdateCustBalCdata.setInt(7, c.c_id);

            int result = payUpdateCustBalCdata.executeUpdate();

            if (result == 0) {
                throw new RuntimeException("Error in PYMNT Txn updating Customer C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);
            }
            //response.setUpdbc(result);
        }
    }

    private void updateBalance(Connection conn, int customerDistrictID, int customerWarehouseID, Customer c) throws SQLException {

        try (PreparedStatement payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL)) {
            payUpdateCustBal.setDouble(1, c.c_balance);
            payUpdateCustBal.setDouble(2, c.c_ytd_payment);
            payUpdateCustBal.setInt(3, c.c_payment_cnt);
            payUpdateCustBal.setInt(4, customerWarehouseID);
            payUpdateCustBal.setInt(5, customerDistrictID);
            payUpdateCustBal.setInt(6, c.c_id);

            int result = payUpdateCustBal.executeUpdate();

            if (result == 0) {
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            }
            //response.setUpdb(result);
        }
    }

    private void insertHistory(Connection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, String w_name, String d_name, Customer c) throws SQLException {
        if (w_name.length() > 10) {
            w_name = w_name.substring(0, 10);
        }
        if (d_name.length() > 10) {
            d_name = d_name.substring(0, 10);
        }
        String h_data = w_name + "    " + d_name;

        try (PreparedStatement payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL)) {
            payInsertHist.setInt(1, customerDistrictID);
            payInsertHist.setInt(2, customerWarehouseID);
            payInsertHist.setInt(3, c.c_id);
            payInsertHist.setInt(4, districtID);
            payInsertHist.setInt(5, w_id);
            payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            payInsertHist.setDouble(7, paymentAmount);
            payInsertHist.setString(8, h_data);
            int result = payInsertHist.executeUpdate();
            if (result == 0) {
                LOG.warn("payment history not inserted");
            }
            //response.setInshist(result);
        }

    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public Customer getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn) throws SQLException {

        try (PreparedStatement payGetCust = this.getPreparedStatement(conn, payGetCustSQL)) {

            payGetCust.setInt(1, c_w_id);
            payGetCust.setInt(2, c_d_id);
            payGetCust.setInt(3, c_id);

            try (ResultSet rs = payGetCust.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                }

                Customer c = TPCCUtil.newCustomerFromResults(rs);
                c.c_id = c_id;
                c.c_last = rs.getString("C_LAST");
                return c;
            }
        }
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public Customer getCustomerByName(int c_w_id, int c_d_id, String customerLastName, Connection conn) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<>();

        try (PreparedStatement customerByName = this.getPreparedStatement(conn, customerByNameSQL)) {

            customerByName.setInt(1, c_w_id);
            customerByName.setInt(2, c_d_id);
            customerByName.setString(3, customerLastName);
            try (ResultSet rs = customerByName.executeQuery()) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("C_LAST={} C_D_ID={} C_W_ID={}", customerLastName, c_d_id, c_w_id);
                }

                while (rs.next()) {
                    Customer c = TPCCUtil.newCustomerFromResults(rs);
                    c.c_id = rs.getInt("C_ID");
                    c.c_last = customerLastName;
                    customers.add(c);
                }
            }
        }

        if (customers.size() == 0) {
            throw new RuntimeException("C_LAST=" + customerLastName + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that
        // counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }
}

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
import com.oltpbenchmark.benchmarks.tpccnorpc.pojo.Customer;
import com.oltpbenchmark.benchmarks.tpccnorpc.pojo.District;
import com.oltpbenchmark.benchmarks.tpccnorpc.pojo.Warehouse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class Payment extends TPCCProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(Payment.class);

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

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int mergeSize, int terminalDistrictLowerID, int terminalDistrictUpperID, TPCCnorpcWorker worker) throws SQLException {

        // The implementation of merged payment transaction
        int districtID = TPCCUtil.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        float[] paymentAmounts = new float[mergeSize];
        float paymentAmount; 
        float total = 0.0f;

        for (int i = 0; i < mergeSize; i++) {
            paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, gen) / 100.0);
            paymentAmounts[i] = paymentAmount;
            total += paymentAmount;
        }

        //updateWarehouse(conn, w_id, paymentAmount);
        updateWarehouse(conn, w_id, total);

        Warehouse w = getWarehouse(conn, w_id);

        //updateDistrict(conn, w_id, districtID, paymentAmount);
        updateDistrict(conn, w_id, districtID, total);

        District d = getDistrict(conn, w_id, districtID);

        //int x = TPCCUtil.randomNumber(1, 100, gen);

        //int customerDistrictID = getCustomerDistrictId(gen, districtID, x);
        //int customerWarehouseID = getCustomerWarehouseID(gen, w_id, numWarehouses, x);

        // generate customer d_id/w_id either from local warehouse or from remote warehouse 
        // and from local district or different district (85% from local wh and dist)
        int[] customerDistrictIDs = new int[mergeSize];
        int[] customerWarehouseIDs = new int[mergeSize];
        for (int i = 0; i < mergeSize; i++) {
            int x = TPCCUtil.randomNumber(1, 100, gen);
            customerDistrictIDs[i] = getCustomerDistrictId(gen, districtID, x);
            customerWarehouseIDs[i] = getCustomerWarehouseID(gen, w_id, numWarehouses, x);
        }

        // todo: possible duplicated users?
        ArrayList<Customer> customers = new ArrayList<>();
        //Customer c = getCustomer(conn, gen, customerDistrictID, customerWarehouseID, paymentAmount);
        customers = getCustomers(conn, gen, mergeSize, customerDistrictIDs, customerWarehouseIDs, paymentAmounts);
        if (customers.size() != mergeSize) {
            throw new RuntimeException("getcustomers size not equal to payment merge size!");
        }

        // if (c.c_credit.equals("BC")) {
        //     // bad credit
        //     c.c_data = getCData(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, c);

        //     updateBalanceCData(conn, customerDistrictID, customerWarehouseID, c);

        // } else {
        //     // GoodCredit

        //     updateBalance(conn, customerDistrictID, customerWarehouseID, c);

        // }
        
        Map<String, Float> cus_pay = new HashMap<>();
        Map<String, Customer> bccustomers = new HashMap<>();
        ArrayList<Customer> gccustomers = new ArrayList<>();
        ArrayList<Customer> newcdata_cus = new ArrayList<>();
        Customer c;
        for (int i = 0; i < mergeSize; i++) {
            c = customers.get(i);
            if (c.c_credit.equals("BC")) {
                // bad credit
                String cus_info = String.valueOf(c.c_w_id) + "_" + String.valueOf(c.c_d_id) + "_" + String.valueOf(c.c_id);
                cus_pay.put(cus_info, paymentAmounts[i]);
                bccustomers.put(cus_info, c);
            } else {
                // GoodCredit
                gccustomers.add(c);   
            }
        }
        // get customer Cdata in batch and
        // update customer balance and cdata if any
        if (!bccustomers.isEmpty()) {
            newcdata_cus = getCDatas(conn, w_id, districtID, cus_pay, bccustomers);
            updateBalanceCDatas(conn, newcdata_cus);
        }
        // update customer balance if any
        if (!gccustomers.isEmpty()) {
            updateBalances(conn, gccustomers);
        }

        //insertHistory(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, w.w_name, d.d_name, c);
        insertHistorys(conn, mergeSize, w_id, districtID, paymentAmounts, w.w_name, d.d_name, customers);

    }

    private int getCustomerWarehouseID(Random gen, int w_id, int numWarehouses, int x) {
        int customerWarehouseID;
        if (x <= 85) {
            customerWarehouseID = w_id;
        } else {
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, numWarehouses, gen);
            }
            while (customerWarehouseID == w_id && numWarehouses > 1);
        }
        return customerWarehouseID;
    }

    private int getCustomerDistrictId(Random gen, int districtID, int x) {
        if (x <= 85) {
            return districtID;
        } else {
            return TPCCUtil.randomNumber(1, TPCCConfig.configDistPerWhse, gen);
        }


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

        return c;
    }

    private ArrayList<Customer> getCustomers(Connection conn, Random gen, int mergeSize, int[] customerDistrictIDs, int[] customerWarehouseIDs, float[] paymentAmounts) throws SQLException {
        ArrayList<Customer> customers = new ArrayList<>();
        ArrayList<String> customerNames = new ArrayList<>();
        ArrayList<Integer> customeIDs = new ArrayList<>();
        String name = "";
        int id = 0;
        int y, names, ids;
        y = names = ids = 0;

        for (int i = 0; i < mergeSize; i++) {
            y = TPCCUtil.randomNumber(1, 100, gen);
            // generate nonduplicate names or ids: need to consider duplicated customers?
            if (y <= 60) {
                // 60% lookups by last name
                name = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
                while (customerNames.contains(name)) {
                    name = TPCCUtil.getNonUniformRandomLastNameForRun(gen);
                }
                customerNames.add(name);
            } else {
                // 40% lookups by customer ID
                id = TPCCUtil.getCustomerID(gen);
                while (customeIDs.contains(id)) {
                    id = TPCCUtil.getCustomerID(gen);
                }
                customeIDs.add(id);
            }
        }

        // get the customers by their last names
        if (!customerNames.isEmpty()) {
            names = customerNames.size();
            ArrayList<Customer> cusbyname = getCustomerByNames(
                Arrays.copyOfRange(customerWarehouseIDs, 0, names), 
                Arrays.copyOfRange(customerDistrictIDs, 0, names), 
                Arrays.copyOfRange(paymentAmounts, 0, names), 
                customerNames, conn);
            customers.addAll(cusbyname);
        }
        // get the customers by their ids
        if (!customeIDs.isEmpty()) {
            ids = customeIDs.size();
            int upper = names + ids;
            ArrayList<Customer> cusbyid = getCustomerByIds(
                Arrays.copyOfRange(customerWarehouseIDs, names, upper), 
                Arrays.copyOfRange(customerDistrictIDs, names, upper), 
                Arrays.copyOfRange(paymentAmounts, names, upper), 
                customeIDs, conn);
            customers.addAll(cusbyid);
        }

        return customers;
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

    private ArrayList<Customer> getCDatas(Connection conn, int w_id, int districtID, 
                                        Map<String, Float> cus_pay, Map<String, Customer> bccustomers) throws SQLException {
        String payGetCustCdataMXSQL =
            "SELECT C_W_ID, C_D_ID, C_ID, C_DATA " +
            "FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE (C_W_ID, C_D_ID, C_ID) IN (";

        ArrayList<Customer> customers = new ArrayList<>();

        for (int i = 0; i < bccustomers.size() - 1; i++) {
            payGetCustCdataMXSQL += "(?, ?, ?),";
        }
        payGetCustCdataMXSQL += "(?, ?, ?))";

        SQLStmt payGetCustCdataMX = new SQLStmt(payGetCustCdataMXSQL);

        try (PreparedStatement payGetCustCdatas = this.getPreparedStatement(conn, payGetCustCdataMX)) {
            String c_data, cus_info;
            int ccwid, ccdid, ccid;
            int i = 0;
            for (Customer c : bccustomers.values()) {
                payGetCustCdatas.setInt(i*3 + 1, c.c_w_id);
                payGetCustCdatas.setInt(i*3 + 2, c.c_d_id);
                payGetCustCdatas.setInt(i*3 + 3, c.c_id);
                i++;
            }

            try (ResultSet rs = payGetCustCdatas.executeQuery()) {
                Customer c;
                while (rs.next()) {
                    ccid = rs.getInt("c_id");
                    ccdid = rs.getInt("c_d_id");
                    ccwid = rs.getInt("c_w_id");
                    c_data = rs.getString("C_DATA");
                    cus_info = String.valueOf(ccwid) + "_" + String.valueOf(ccdid) + "_" + String.valueOf(ccid);
                    c_data = ccid + " " + ccdid + " " + ccwid + " " + districtID + " " + w_id + " " + cus_pay.get(cus_info) + " | " + c_data;
                    if (c_data.length() > 500) {
                        c_data = c_data.substring(0, 500);
                    }
                    c = bccustomers.get(cus_info);
                    c.c_data = c_data;
                    customers.add(c);
                }
            }
            return customers;
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
        }
    }

    private void updateBalanceCDatas(Connection conn, ArrayList<Customer> cus_list) throws SQLException {
        String payUpdateCustBalCdataMXSQL = 
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            " SET C_BALANCE = CASE ";
            // "C_YTD_PAYMENT = ?, " +
            // "C_PAYMENT_CNT = ?, " +
            // "C_DATA = ? " +
            // "WHERE C_W_ID = ? " +
            // "AND C_D_ID = ? " +
            // "AND C_ID = ?";

        Customer c;
        int cSize = cus_list.size();

        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalCdataMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalCdataMXSQL += " ELSE C_BALANCE END, C_YTD_PAYMENT = CASE ";
        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalCdataMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalCdataMXSQL += " ELSE C_YTD_PAYMENT END, C_PAYMENT_CNT = CASE ";
        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalCdataMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalCdataMXSQL += " ELSE C_PAYMENT_CNT END, C_DATA = CASE ";
        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalCdataMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalCdataMXSQL += " ELSE C_DATA END WHERE (C_W_ID, C_D_ID, C_ID) IN (";
        for (int i = 0; i < cSize - 1; i++) {
            payUpdateCustBalCdataMXSQL += "(?,?,?),";
        }
        payUpdateCustBalCdataMXSQL += "(?,?,?))";

        SQLStmt payUpdateCustBalCdataMX = new SQLStmt(payUpdateCustBalCdataMXSQL);

        try (PreparedStatement payUpdateCustBalCdatas = this.getPreparedStatement(conn, payUpdateCustBalCdataMX)) {
            for (int num = 0; num < cSize; num++) {
                c = cus_list.get(num);
                payUpdateCustBalCdatas.setInt(num * 4 + 1, c.c_w_id);
                payUpdateCustBalCdatas.setInt(num * 4 + 2, c.c_d_id);
                payUpdateCustBalCdatas.setInt(num * 4 + 3, c.c_id);
                payUpdateCustBalCdatas.setDouble(num * 4 + 4, c.c_balance);

                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + 3, c.c_id);
                payUpdateCustBalCdatas.setDouble(num * 4 + cSize*4 + 4, c.c_ytd_payment);

                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + 3, c.c_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + 4, c.c_payment_cnt);

                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBalCdatas.setInt(num * 4 + cSize*4 + cSize*4 + cSize*4 + 3, c.c_id);
                payUpdateCustBalCdatas.setString(num * 4 + cSize*4 + cSize*4 + cSize*4 + 4, c.c_data);

                payUpdateCustBalCdatas.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBalCdatas.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBalCdatas.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + cSize*4 + 3, c.c_id);
            }

            int result = payUpdateCustBalCdatas.executeUpdate();

            if (result == 0) {
                throw new RuntimeException("payUpdateCustBalCdatas: Error in PYMNT Merge Txn updating Customer");
            }
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
        }
    }

    private void updateBalances(Connection conn, ArrayList<Customer> cus_list) throws SQLException {

        String payUpdateCustBalMXSQL =
            "UPDATE " + TPCCConstants.TABLENAME_CUSTOMER +
            " SET C_BALANCE = CASE ";
            // "WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? " +
            // "ELSE C_BALANCE END, " +
            // "C_YTD_PAYMENT = CASE " +
            // "WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? " +
            // "ELSE C_YTD_PAYMENT END, " +
            // "C_PAYMENT_CNT = CASE " +
            // "WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? " +
            // "ELSE C_PAYMENT_CNT END " +
            // "WHERE (C_W_ID, C_D_ID, C_ID) IN ((?,?,?)...)";

        Customer c;
        int cSize = cus_list.size();

        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalMXSQL += " ELSE C_BALANCE END, C_YTD_PAYMENT = CASE ";
        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalMXSQL += " ELSE C_YTD_PAYMENT END, C_PAYMENT_CNT = CASE ";
        for (int i = 0; i < cSize; i++) {
            payUpdateCustBalMXSQL += " WHEN C_W_ID = ? AND C_D_ID = ? AND C_ID = ? THEN ? ";
        }
        payUpdateCustBalMXSQL += " ELSE C_PAYMENT_CNT END WHERE (C_W_ID, C_D_ID, C_ID) IN (";
        for (int i = 0; i < cSize - 1; i++) {
            payUpdateCustBalMXSQL += "(?,?,?),";
        }
        payUpdateCustBalMXSQL += "(?,?,?))";
        SQLStmt payUpdateCustBalMX = new SQLStmt(payUpdateCustBalMXSQL);

        try (PreparedStatement payUpdateCustBals = this.getPreparedStatement(conn, payUpdateCustBalMX)) {
            for (int num = 0; num < cSize; num++) {
                c = cus_list.get(num);
                payUpdateCustBals.setInt(num * 4 + 1, c.c_w_id);
                payUpdateCustBals.setInt(num * 4 + 2, c.c_d_id);
                payUpdateCustBals.setInt(num * 4 + 3, c.c_id);
                payUpdateCustBals.setDouble(num * 4 + 4, c.c_balance);

                payUpdateCustBals.setInt(num * 4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBals.setInt(num * 4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBals.setInt(num * 4 + cSize*4 + 3, c.c_id);
                payUpdateCustBals.setDouble(num * 4 + cSize*4 + 4, c.c_ytd_payment);

                payUpdateCustBals.setInt(num * 4 + cSize*4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBals.setInt(num * 4 + cSize*4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBals.setInt(num * 4 + cSize*4 + cSize*4 + 3, c.c_id);
                payUpdateCustBals.setInt(num * 4 + cSize*4 + cSize*4 + 4, c.c_payment_cnt);

                payUpdateCustBals.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + 1, c.c_w_id);
                payUpdateCustBals.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + 2, c.c_d_id);
                payUpdateCustBals.setInt(num * 3 + cSize*4 + cSize*4 + cSize*4 + 3, c.c_id);
            }

            int result = payUpdateCustBals.executeUpdate();

            if (result == 0) {
                throw new RuntimeException("payUpdateCustBals: Error in PYMNT Merge Txn updating Customer");
            }
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
            payInsertHist.executeUpdate();
        }
    }

    private void insertHistorys(Connection conn, int mergeSize, int w_id, int districtID, float[] paymentAmounts, String w_name, String d_name, ArrayList<Customer> cus_list) throws SQLException { 
        String payInsertHistMXSQL =
            "INSERT INTO " + TPCCConstants.TABLENAME_HISTORY +
            " (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA) " +
            "VALUES ";

        Customer c;
        if (w_name.length() > 10) {
            w_name = w_name.substring(0, 10);
        }
        if (d_name.length() > 10) {
            d_name = d_name.substring(0, 10);
        }
        String h_data = w_name + "    " + d_name;

        for (int i = 0; i < mergeSize - 1; i++) {
            payInsertHistMXSQL += "(?,?,?,?,?,?,?,?),";
        }
        payInsertHistMXSQL += "(?,?,?,?,?,?,?,?)";
        SQLStmt payInsertHistMX = new SQLStmt(payInsertHistMXSQL);

        try (PreparedStatement payInsertHists = this.getPreparedStatement(conn, payInsertHistMX)) {
            for (int i = 0; i < mergeSize; i++) {
                c = cus_list.get(i);
                payInsertHists.setInt(i*8 +1, c.c_d_id);
                payInsertHists.setInt(i*8 +2, c.c_w_id);
                payInsertHists.setInt(i*8 +3, c.c_id);
                payInsertHists.setInt(i*8 +4, districtID);
                payInsertHists.setInt(i*8 +5, w_id);
                payInsertHists.setTimestamp(i*8 +6, new Timestamp(System.currentTimeMillis()));
                payInsertHists.setDouble(i*8 +7, paymentAmounts[i]);
                payInsertHists.setString(i*8 +8, h_data);
            }
            int result = payInsertHists.executeUpdate();
            if (result == 0) {
                throw new RuntimeException("payInsertHists: Error in payInsertHists");
            }
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

    public ArrayList<Customer> getCustomerByIds(int[] c_w_id, int[] c_d_id, float[] paymentAmounts, ArrayList<Integer> c_id, Connection conn) throws SQLException {
        String payGetCustMXSQL =
            "SELECT C_W_ID, C_D_ID, C_ID, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, " +
            "C_CITY, C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, " +
            "C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE (C_W_ID, C_D_ID, C_ID) IN (";

        ArrayList<Customer> customers = new ArrayList<>();
        Customer c;
        int idSize = c_id.size();
        for (int i = 0; i < idSize - 1; i++) {
            payGetCustMXSQL += "(?, ?, ?),";
        }
        payGetCustMXSQL += "(?, ?, ?))";
        SQLStmt payGetCustMX = new SQLStmt(payGetCustMXSQL);

        try (PreparedStatement payGetCust = this.getPreparedStatement(conn, payGetCustMX)) {
            for (int i = 0; i < idSize; i++) {
                payGetCust.setInt(i*3 + 1, c_w_id[i]);
                payGetCust.setInt(i*3 + 2, c_d_id[i]);
                payGetCust.setInt(i*3 + 3, c_id.get(i));
            }

            try (ResultSet rs = payGetCust.executeQuery()) {
                while (rs.next()) {
                    c = TPCCUtil.newPayCustomerFromResults(rs);
                    customers.add(c);
                }
            }
        }

        if (customers.size() < idSize) {
            throw new RuntimeException("not enough customerbyIDs found!");
        } else {
            // update customer payment
            for (int i = 0; i < idSize; i++) {
                c = customers.get(i);
                c.c_balance -= paymentAmounts[i];
                c.c_ytd_payment += paymentAmounts[i];
                c.c_payment_cnt += 1;
            }
        }
        return customers;
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

    public ArrayList<Customer> getCustomerByNames(int[] c_w_id, int[] c_d_id, float[] paymentAmounts, ArrayList<String> customerLastName, Connection conn) throws SQLException {
        String customerByNameMXSQL = 
            "SELECT  C_W_ID, C_D_ID, C_LAST, C_FIRST, C_MIDDLE, C_ID, C_STREET_1, C_STREET_2, C_CITY, " +
            "C_STATE, C_ZIP, C_PHONE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, " +
            "C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_SINCE " +
            "FROM " + TPCCConstants.TABLENAME_CUSTOMER +
            " WHERE (C_W_ID, C_D_ID, C_LAST) IN (";

        ArrayList<Customer> customers = new ArrayList<>();
        ArrayList<Customer> returns = new ArrayList<>();

        String name = "";
        String currentName = "";
        Customer c;
        int currentNameCount = 0;
        int nameSize = customerLastName.size();

        for (int i = 0; i < nameSize - 1; i++) {
            customerByNameMXSQL += "(?, ?, ?),";
        }
        customerByNameMXSQL += "(?, ?, ?)) ORDER BY C_LAST, C_FIRST";
        SQLStmt customerByNameMX = new SQLStmt(customerByNameMXSQL);

        try (PreparedStatement customerByName = this.getPreparedStatement(conn, customerByNameMX)) {
            for (int i = 0; i < nameSize; i++) {
                customerByName.setInt(i*3 + 1, c_w_id[i]);
                customerByName.setInt(i*3 + 2, c_d_id[i]);
                customerByName.setString(i*3 + 3, customerLastName.get(i));
            }
            try (ResultSet rs = customerByName.executeQuery()) {
                while (rs.next()) {
                    c = TPCCUtil.newPayCustomerFromResults(rs);
                    customers.add(c);
                }
            }
        }

        if (customers.size() < nameSize) {
            throw new RuntimeException("not enough customerbynames found!");
        } else {
            int start = 0;
            for (int i = 0; i < customers.size(); i++) {
                c = customers.get(i);
                currentName = c.c_last;
                if (currentName.equals(name)) {
                    currentNameCount++;
                } else {
                    if (!name.isEmpty()) {
                        int index = currentNameCount / 2;
                        if (currentNameCount % 2 == 0) {
                            index -= 1;
                        }
                        returns.add(customers.get(start+index));
                        start += currentNameCount;
                    }
                    name = currentName;
                    currentNameCount = 1;
                }
            }

            if (start < customers.size()) {
                int index = currentNameCount / 2;
                if (currentNameCount % 2 == 0) {
                    index -= 1;
                }
                returns.add(customers.get(start+index));
            }
        }
        
        if (returns.size() != nameSize) {
            throw new RuntimeException("not enough customerbynames return!");
        } else {
            // update customer payment
            for (int i = 0; i < nameSize; i++) {
                c = returns.get(i);
                c.c_balance -= paymentAmounts[i];
                c.c_ytd_payment += paymentAmounts[i];
                c.c_payment_cnt += 1;
            }
        }

        return returns;
    }


}

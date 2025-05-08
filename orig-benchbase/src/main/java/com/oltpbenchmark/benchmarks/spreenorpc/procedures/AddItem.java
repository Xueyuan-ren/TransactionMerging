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

package com.oltpbenchmark.benchmarks.spreenorpc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeConfig;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeUtil;
import com.oltpbenchmark.benchmarks.spreenorpc.SpreeWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class AddItem extends SpreeMergedProcedure {

    private static final Logger LOG = LoggerFactory.getLogger(AddItem.class);

    public final SQLStmt stmtLoadPriceSQL = new SQLStmt(
            "SELECT * FROM spree_prices " +
            "WHERE spree_prices.deleted_at IS NULL " +
            "AND spree_prices.variant_id = ? " +
            "AND spree_prices.currency = ? LIMIT 1");
 
    public final SQLStmt stmtLoadLineItemSQL = new SQLStmt(
            "SELECT * FROM spree_line_items " +
            "WHERE spree_line_items.order_id = ? " +
            "AND spree_line_items.variant_id = ? " +
            "ORDER BY spree_line_items.created_at ASC LIMIT 1");
 
    public final SQLStmt stmtLoadTaxCategorySQL = new SQLStmt(
            "SELECT * FROM spree_tax_categories " +
            "WHERE spree_tax_categories.deleted_at IS NULL " +
            "AND spree_tax_categories.id = ? LIMIT 1");
 
    public final SQLStmt stmtLoadProductSQL = new SQLStmt(
            "SELECT * FROM spree_products " +
            "WHERE spree_products.id = ? LIMIT 1");

    public final SQLStmt stmtCheckStockSQL = new SQLStmt(
            "SELECT SUM(spree_stock_items.count_on_hand) FROM spree_stock_items " +
            "INNER JOIN spree_stock_locations ON spree_stock_locations.deleted_at IS NULL " +
            "AND spree_stock_locations.id = spree_stock_items.stock_location_id " +
            "WHERE spree_stock_items.deleted_at IS NULL " +
            "AND spree_stock_items.variant_id = ? " +
            "AND spree_stock_locations.deleted_at IS NULL " +
            "AND spree_stock_locations.active = 1");

    public final SQLStmt stmtInsertLineItemSQL = new SQLStmt(
            "INSERT INTO spree_line_items " +
            "(variant_id, order_id, quantity, price, created_at, updated_at, currency) " +
            "VALUES (?,?,?,?,?,?,?)");

    public final SQLStmt stmtUpdateLineItemSQL = new SQLStmt(
            "UPDATE spree_line_items " +
            "SET spree_line_items.pre_tax_amount = ? " +
            "WHERE spree_line_items.id = ?");

    public final SQLStmt stmtSumLineItemQuantitySQL = new SQLStmt(
            "SELECT SUM(spree_line_items.quantity) FROM spree_line_items " +
            "WHERE spree_line_items.order_id = ?");
    
    public final SQLStmt stmtSumLineItemTotalSQL = new SQLStmt(
            "SELECT SUM(price * quantity) FROM spree_line_items " +
            "WHERE spree_line_items.order_id = ?");

    public final SQLStmt stmtUpdateOrderDetailsSQL = new SQLStmt(
             "UPDATE spree_orders " +
             "SET spree_orders.item_total = ?, " +
             "spree_orders.item_count = ?, " +
             "spree_orders.total = ?, " +
             "spree_orders.updated_at = ? " +
             "WHERE spree_orders.id = ?");

    public final SQLStmt stmtUpdateOrderSQL = new SQLStmt(
            "UPDATE spree_orders " +
            "SET spree_orders.updated_at = ? " +
            "WHERE spree_orders.id = ?");


    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int mergeSize,
        int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) 
        throws SQLException {

        int numItems = SpreeConfig.configItemCount;
        int total_orders = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        addItemTransaction(numItems, total_orders, mergeSize, gen, conn);

    }

    private void  addItemTransaction(int numItems, int total_orders, int mergeSize, Random gen, Connection conn)
            throws SQLException {

        // merged statement semantics
        String stmtLoadPriceMTSQL = 
            "SELECT * FROM spree_prices " +
            "WHERE spree_prices.deleted_at IS NULL " +
            "AND spree_prices.currency = ? " +
            "AND spree_prices.variant_id IN (";
        
        String stmtLoadLineItemMTSQL = 
            "SELECT * FROM spree_line_items " +
            "WHERE (order_id, variant_id) IN (";
            //"ORDER BY spree_line_items.created_at ASC LIMIT 1");
        
        String stmtLoadTaxCategoryMTSQL = 
            "SELECT * FROM spree_tax_categories " +
            "WHERE spree_tax_categories.deleted_at IS NULL " +
            "AND spree_tax_categories.id IN (";

        String stmtLoadProductMTSQL = 
            "SELECT * FROM spree_products " +
            "WHERE spree_products.id IN (";

        String stmtCheckStockMTSQL = 
            "SELECT spree_stock_items.variant_id, SUM(spree_stock_items.count_on_hand) " +
            "FROM spree_stock_items " +
            "INNER JOIN spree_stock_locations " +
            "ON spree_stock_locations.deleted_at IS NULL " +
            "AND spree_stock_locations.id = spree_stock_items.stock_location_id " +
            "WHERE spree_stock_locations.deleted_at IS NULL " +
            "AND spree_stock_locations.active = 1 " +
            "AND spree_stock_items.deleted_at IS NULL " +
            "AND spree_stock_items.variant_id IN (";

        String stmtInsertLineItemMTSQL = 
            "INSERT INTO spree_line_items (variant_id, order_id, quantity, price, created_at, updated_at, currency) " +
            " VALUES ";

        String stmtUpdateLineItemMTSQL = 
            "UPDATE spree_line_items " +
            "SET spree_line_items.pre_tax_amount = CASE ";

        String stmtSumLineItemQuantityMTSQL = 
            "SELECT order_id, SUM(spree_line_items.quantity) AS sum_quantity FROM spree_line_items " +
            "WHERE spree_line_items.order_id IN (";
            //Group by spree_line_items.order_id

        String stmtSumLineItemTotalMTSQL = 
            "SELECT order_id, SUM(price * quantity) AS total FROM spree_line_items " +
            "WHERE spree_line_items.order_id IN (";
            //Group by spree_line_items.order_id

        String stmtUpdateOrderDetailsMTSQL = 
            "UPDATE spree_orders " +
            "SET spree_orders.item_total = CASE ";
            //"spree_orders.item_count = ?, " +
            //"spree_orders.total = ?, " +
            //"spree_orders.updated_at = ? " +
            //"WHERE spree_orders.id = ?";

        String stmtUpdateOrderMTSQL = 
            "UPDATE spree_orders " +
            "SET spree_orders.updated_at = ? " +
            "WHERE spree_orders.id IN (";

        // concatenate statements
        for (int i = 0; i < mergeSize - 1; i++) {
            stmtLoadPriceMTSQL += "?,";
            stmtLoadLineItemMTSQL += "(?,?),";
            stmtLoadTaxCategoryMTSQL += "?,";
            stmtLoadProductMTSQL += "?,";
            stmtCheckStockMTSQL += "?,";
            stmtInsertLineItemMTSQL += "(?,?,?,?,?,?,?),";
            stmtSumLineItemQuantityMTSQL += "?,";
            stmtSumLineItemTotalMTSQL += "?,";
            stmtUpdateOrderMTSQL += "?,";
        }
        stmtLoadPriceMTSQL += "?)";
        stmtLoadLineItemMTSQL += "(?,?))";
        stmtLoadTaxCategoryMTSQL += "?)";
        stmtLoadProductMTSQL += "?)";
        stmtCheckStockMTSQL += "?) Group by spree_stock_items.variant_id";
        stmtInsertLineItemMTSQL += "(?,?,?,?,?,?,?)";
        stmtSumLineItemQuantityMTSQL += "?) Group by spree_line_items.order_id";
        stmtSumLineItemTotalMTSQL += "?) Group by spree_line_items.order_id";
        stmtUpdateOrderMTSQL += "?)";

        // concatenate update case statements
        for (int i = 0; i < mergeSize; i++) {
            stmtUpdateLineItemMTSQL += "WHEN spree_line_items.id = ? THEN ? ";
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateLineItemMTSQL += "ELSE spree_line_items.pre_tax_amount END WHERE spree_line_items.id IN (";
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.item_total END, spree_orders.item_count = CASE ";
        for (int i = 0; i < mergeSize; i++) {
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.item_count END, spree_orders.total = CASE ";
        for (int i = 0; i < mergeSize; i++) {
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.total END, spree_orders.updated_at = ? WHERE spree_orders.id IN (";
        for (int i = 0; i < mergeSize - 1; i++) {
            stmtUpdateLineItemMTSQL += "?,";
            stmtUpdateOrderDetailsMTSQL += "?,";
        }
        stmtUpdateLineItemMTSQL += "?)";
        stmtUpdateOrderDetailsMTSQL += "?)";

        SQLStmt stmtLoadPriceMT = new SQLStmt(stmtLoadPriceMTSQL);
        SQLStmt stmtLoadLineItemMT = new SQLStmt(stmtLoadLineItemMTSQL);
        SQLStmt stmtLoadTaxCategoryMT = new SQLStmt(stmtLoadTaxCategoryMTSQL);
        SQLStmt stmtLoadProductMT = new SQLStmt(stmtLoadProductMTSQL);
        SQLStmt stmtCheckStockMT = new SQLStmt(stmtCheckStockMTSQL);
        SQLStmt stmtInsertLineItemMT = new SQLStmt(stmtInsertLineItemMTSQL);
        SQLStmt stmtUpdateLineItemMT = new SQLStmt(stmtUpdateLineItemMTSQL);
        SQLStmt stmtSumLineItemQuantityMT = new SQLStmt(stmtSumLineItemQuantityMTSQL);
        SQLStmt stmtSumLineItemTotalMT = new SQLStmt(stmtSumLineItemTotalMTSQL);
        SQLStmt stmtUpdateOrderDetailsMT = new SQLStmt(stmtUpdateOrderDetailsMTSQL);
        SQLStmt stmtUpdateOrderMT = new SQLStmt(stmtUpdateOrderMTSQL);
        

        try (PreparedStatement stmtLoadPrice = this.getPreparedStatement(conn, stmtLoadPriceMT);
            PreparedStatement stmtLoadLineItem = this.getPreparedStatement(conn, stmtLoadLineItemMT);
            PreparedStatement stmtLoadTaxCategory = this.getPreparedStatement(conn, stmtLoadTaxCategoryMT);
            PreparedStatement stmtLoadProduct = this.getPreparedStatement(conn, stmtLoadProductMT);
            PreparedStatement stmtCheckStock = this.getPreparedStatement(conn, stmtCheckStockMT);
            PreparedStatement stmtInsertLineItem = this.getPreparedStatementReturnKeys(conn, stmtInsertLineItemMT, new int[]{mergeSize});
            PreparedStatement stmtUpdateLineItem = this.getPreparedStatement(conn, stmtUpdateLineItemMT);
            PreparedStatement stmtSumLineItemQuantity = this.getPreparedStatement(conn, stmtSumLineItemQuantityMT);
            PreparedStatement stmtSumLineItemTotal = this.getPreparedStatement(conn, stmtSumLineItemTotalMT);
            PreparedStatement stmtUpdateOrderDetails = this.getPreparedStatement(conn, stmtUpdateOrderDetailsMT);
            PreparedStatement stmtUpdateOrder = this.getPreparedStatement(conn, stmtUpdateOrderMT)) {
            
            // set parameters
            int variant_id = 0;
            int order_id = 0;
            String currency = "USD";
            double defprice = -1.0;
            double price = 0.0;

            int[] variant_ids = new int[mergeSize];
            ArrayList<Integer> order_ids = new ArrayList<>(mergeSize);
            Map<Integer, Double> variant_price = new HashMap<>();
            Map<Integer, Integer> order_quantity = new HashMap<>();
            Map<Integer, Double> order_itemTotal = new HashMap<>();

            for (int i = 0; i < mergeSize; i++) {
                variant_id = SpreeUtil.randomNumber(1, numItems, gen);
                variant_price.put(variant_id, defprice);        
                variant_ids[i] = variant_id;
            }
            // non-duplicate variants/products count
            int nondup = variant_price.size();

            //load price
            //TODO: check spree_prices index, maybe using (variant_id, currency) is more efficient
            stmtLoadPrice.setString(1, currency);
            for (int i = 0; i < mergeSize; i++) {
                stmtLoadPrice.setInt(i + 2, variant_ids[i]);
            }
            int priceCount = 0;
            try (ResultSet rs = stmtLoadPrice.executeQuery()) {
                while (rs.next()) {
                    price = rs.getDouble("amount");
                    variant_id = rs.getInt("variant_id");
                    variant_price.put(variant_id, price);
                    priceCount++;
                }
            }
            // check if each price exists for all non-duplicate variants
            if (priceCount != nondup) {
                int missid = 0;
                for (Map.Entry<Integer, Double> entry : variant_price.entrySet()) {
                    if (entry.getValue().equals(defprice)) {
                        missid = entry.getKey();
                        break;
                    }
                }
                throw new RuntimeException("The price for variant_id " + missid + " not exist!");
            }

            //load lineitem
            for (int i = 0; i < mergeSize; i++) {
                order_id = SpreeUtil.randomNumber(1, total_orders, gen);
                while (order_ids.contains(order_id)) {
                    order_id = SpreeUtil.randomNumber(1, total_orders, gen);
                }
                order_ids.add(order_id);
                stmtLoadLineItem.setInt(i*2 + 1, order_id);
                stmtLoadLineItem.setInt(i*2 + 2, variant_ids[i]);
            }

            try (ResultSet rs = stmtLoadLineItem.executeQuery()) {
                //check if lineitem already exists: only one will throw UserAbortException
                //TODO: use branch to deal with different statements in one type of transactons
                if (rs.next()) {
                    int existid = rs.getInt("variant_id");
                    throw new UserAbortException("The lineitem variant_id " + existid + " already exist!");
                }
            }

            //load Tax Category
            int tax_category_id = 1; //only 1 row in the table spree_tax_categories
            for (int i = 0; i < mergeSize; i++) {
                stmtLoadTaxCategory.setInt(i + 1, tax_category_id);
            }
            try (ResultSet rs = stmtLoadTaxCategory.executeQuery()) {
                // if the Tax Category not exist
                if (!rs.next()) {
                    throw new RuntimeException("The tax_category_id " + tax_category_id + " not exist!");
                }
            }

            //load product
            int product_id = 0;
            for (int i = 0; i < mergeSize; i++) {
                product_id = variant_ids[i];
                stmtLoadProduct.setInt(i + 1, product_id);
            }
            int productCount = 0;
            try (ResultSet rs = stmtLoadProduct.executeQuery()) { 
                while (rs.next()) {
                    productCount++;
                }
            }
            // check if the product not exist
            if (productCount != nondup) {
                throw new RuntimeException("Has a product_id not exist!");
            }

            //check stock
            for (int i = 0; i < mergeSize; i++) {
                stmtCheckStock.setInt(i + 1, variant_ids[i]);
            }
            int stockItem = 0;
            try (ResultSet rs = stmtCheckStock.executeQuery()) { 
                while (rs.next()) {
                    stockItem++;
                }
            }
            // if the stock item not exists
            if (stockItem != nondup) {
                throw new RuntimeException("Has a stock variant_id not exist!");
            }

            //insert order line item
            int quantity = 0;
            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
            for (int i = 0; i < mergeSize; i++) {
                quantity = SpreeUtil.randomNumber(1, 10, gen);
                stmtInsertLineItem.setInt(i*7 + 1, variant_ids[i]);
                stmtInsertLineItem.setInt(i*7 + 2, order_ids.get(i));
                stmtInsertLineItem.setInt(i*7 + 3, quantity);
                stmtInsertLineItem.setDouble(i*7 + 4, variant_price.get(variant_ids[i]));
                stmtInsertLineItem.setTimestamp(i*7 + 5, sysdate);
                stmtInsertLineItem.setTimestamp(i*7 + 6, sysdate);
                stmtInsertLineItem.setString(i*7 + 7, currency);
            }
            int result = stmtInsertLineItem.executeUpdate();
            if (result == 0) {
                LOG.error("insert into spree_line_items failed");
                throw new RuntimeException("Error: Cannot insert into spree_line_items!");
            }

            // get returned keys: line_item ids ( in the order as the inserted rows)
            ArrayList<Integer> line_ids = new ArrayList<>(mergeSize);
            try (ResultSet generatedKeys = stmtInsertLineItem.getGeneratedKeys()) {
                while (generatedKeys.next()) {
                    line_ids.add(generatedKeys.getInt(1));
                }   
            }
            if (line_ids.size() != mergeSize) {
                throw new RuntimeException("Some stmtInsertLineItem getting line_id failed!");
            }

            //update the line_item
            //double pre_tax_amount = price;
            int line_id = 0;
            for (int i = 0; i < mergeSize; i++) {
                line_id = line_ids.get(i);
                stmtUpdateLineItem.setInt(i*2 + 1, line_id);
                stmtUpdateLineItem.setDouble(i*2 + 2, variant_price.get(variant_ids[i]));
                stmtUpdateLineItem.setInt(mergeSize*2 + i + 1 , line_id);
            }
            result = stmtUpdateLineItem.executeUpdate();
            if (result == 0) {
                LOG.error("update spree_line_items failed");
                throw new RuntimeException("Error: Cannot update spree_line_items!");
            }

            //get quantity sum for this line_item
            for (int i = 0; i < mergeSize; i++) {
                stmtSumLineItemQuantity.setInt(i + 1, order_ids.get(i));
            }
            try (ResultSet rs = stmtSumLineItemQuantity.executeQuery()) {
                while (rs.next()) {
                    order_quantity.put(rs.getInt("order_id"), rs.getInt("sum_quantity"));
                }
            }
            if (order_quantity.size() != mergeSize) {
                throw new RuntimeException("Some stmtSumLineItemQuantity getting sum_quantity failed!");
            }

            //get total for this line_item
            for (int i = 0; i < mergeSize; i++) {
                stmtSumLineItemTotal.setInt(i + 1, order_ids.get(i));
            }
            try (ResultSet rs = stmtSumLineItemTotal.executeQuery()) {
                while (rs.next()) {
                    order_itemTotal.put(rs.getInt("order_id"), rs.getDouble("total"));
                }
            }
            if (order_itemTotal.size() != mergeSize) {
                throw new RuntimeException("Some stmtSumLineItemTotal getting total failed!");
            }

            //update order details
            for (int i = 0; i < mergeSize; i++) {
                stmtUpdateOrderDetails.setInt(i*2 + 1, order_ids.get(i)); // order_id
                stmtUpdateOrderDetails.setDouble(i*2 + 2, order_itemTotal.get(order_ids.get(i))); // item_total
                
                stmtUpdateOrderDetails.setInt(mergeSize*2 + i*2 + 1, order_ids.get(i)); // order_id
                stmtUpdateOrderDetails.setInt(mergeSize*2 + i*2 + 2, order_quantity.get(order_ids.get(i)));  //item_count

                stmtUpdateOrderDetails.setInt(mergeSize*2 + mergeSize*2 + i*2 + 1, order_ids.get(i)); // order_id
                stmtUpdateOrderDetails.setDouble(mergeSize*2 + mergeSize*2 + i*2 + 2, order_itemTotal.get(order_ids.get(i))); // total
                
                stmtUpdateOrderDetails.setInt(mergeSize*2 + mergeSize*2 + mergeSize*2 + i + 2, order_ids.get(i));
            }
            stmtUpdateOrderDetails.setTimestamp(mergeSize*2 + mergeSize*2 + mergeSize*2 + 1, sysdate);  //updated_at
            result = stmtUpdateOrderDetails.executeUpdate();
            if (result == 0) {
                throw new RuntimeException("Error: Cannot update order details!");
            }

            //update order timestamp
            stmtUpdateOrder.setTimestamp(1, sysdate);
            for (int i = 0; i < mergeSize; i++) {
                stmtUpdateOrder.setInt(i + 2, order_ids.get(i));
            }
            result = stmtUpdateOrder.executeUpdate();
            if (result == 0) {
                throw new RuntimeException("Error: Cannot update updated_at on spree_order!");
            }
        }
    }
}




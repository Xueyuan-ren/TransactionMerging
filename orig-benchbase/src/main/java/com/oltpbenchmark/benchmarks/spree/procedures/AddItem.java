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

package com.oltpbenchmark.benchmarks.spree.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spree.SpreeConfig;
import com.oltpbenchmark.benchmarks.spree.SpreeUtil;
import com.oltpbenchmark.benchmarks.spree.SpreeWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;

public class AddItem extends SpreeProcedure {

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


    public void run(Connection conn, Random gen, int w_id, int numWarehouses,
        int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) 
        throws SQLException {

        int numItems = SpreeConfig.configItemCount;
        int total_orders = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        addItemTransaction(numItems, total_orders, gen, conn);

    }

    private void  addItemTransaction(int numItems, int total_orders, Random gen, Connection conn)
            throws SQLException {

        try (PreparedStatement stmtLoadPrice = this.getPreparedStatement(conn, stmtLoadPriceSQL);
            PreparedStatement stmtLoadLineItem = this.getPreparedStatement(conn, stmtLoadLineItemSQL);
            PreparedStatement stmtLoadTaxCategory = this.getPreparedStatement(conn, stmtLoadTaxCategorySQL);
            PreparedStatement stmtLoadProduct = this.getPreparedStatement(conn, stmtLoadProductSQL);
            PreparedStatement stmtCheckStock = this.getPreparedStatement(conn, stmtCheckStockSQL);
            PreparedStatement stmtInsertLineItem = this.getPreparedStatementReturnKeys(conn, stmtInsertLineItemSQL, new int[]{1});
            PreparedStatement stmtUpdateLineItem = this.getPreparedStatement(conn, stmtUpdateLineItemSQL);
            PreparedStatement stmtSumLineItemQuantity = this.getPreparedStatement(conn, stmtSumLineItemQuantitySQL);
            PreparedStatement stmtSumLineItemTotal = this.getPreparedStatement(conn, stmtSumLineItemTotalSQL);
            PreparedStatement stmtUpdateOrderDetails = this.getPreparedStatement(conn, stmtUpdateOrderDetailsSQL);
            PreparedStatement stmtUpdateOrder = this.getPreparedStatement(conn, stmtUpdateOrderSQL)) {

                //load price
                int variant_id = SpreeUtil.randomNumber(1, numItems, gen);
                String currency = "USD";
                double price = 0.0;
                stmtLoadPrice.setInt(1, variant_id);
                stmtLoadPrice.setString(2, currency);
                try (ResultSet rs = stmtLoadPrice.executeQuery()) {
                    // if the price not exist
                    if (!rs.next()) {
                        throw new RuntimeException("The price variant_id " + variant_id
                                   + " not exist!");
                    }
                    price = rs.getDouble("amount");
                }
                
                //check lineitem existing for current order
                int order_id = SpreeUtil.randomNumber(1, total_orders, gen);
                stmtLoadLineItem.setInt(1, order_id);
                stmtLoadLineItem.setInt(2, variant_id);
                try (ResultSet rs = stmtLoadLineItem.executeQuery()) {
                    // in case the lineitem already exists
                    if (rs.next()) {
                        throw new UserAbortException("The lineitem variant_id " + variant_id
                                   + " already exist!");
                    }
                }

                //load Tax Category
                int tax_category_id = 1;
                stmtLoadTaxCategory.setInt(1, tax_category_id);
                try (ResultSet rs = stmtLoadTaxCategory.executeQuery()) {
                    // if the Tax Category not exist
                    if (!rs.next()) {
                        throw new RuntimeException("The tax_category_id " + tax_category_id
                                   + " not exist!");
                    }
                }

                //load product
                int product_id = variant_id;
                stmtLoadProduct.setInt(1, product_id);
                try (ResultSet rs = stmtLoadProduct.executeQuery()) {
                    // if the product not exist
                    if (!rs.next()) {
                        throw new RuntimeException("The product_id " + product_id
                                   + " not exist!");
                    }
                }

                //check stock
                //int stock_location_id = 1;          
                //int count_on_hand = 0;
                stmtCheckStock.setInt(1, variant_id);
                try (ResultSet rs = stmtCheckStock.executeQuery()) {
                    // if the stock item not exists
                    if (!rs.next()) {
                        throw new RuntimeException("The stock variant_id " + variant_id
                                    + " not exist!");
                    }
                }

                //insert order line item
                int quantity = SpreeUtil.randomNumber(1, 10, gen);
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                stmtInsertLineItem.setInt(1, variant_id);
                stmtInsertLineItem.setInt(2, order_id);
                stmtInsertLineItem.setInt(3, quantity);
                stmtInsertLineItem.setDouble(4, price);
                stmtInsertLineItem.setTimestamp(5, sysdate);
                stmtInsertLineItem.setTimestamp(6, sysdate);
                stmtInsertLineItem.setString(7, currency);
                int result = stmtInsertLineItem.executeUpdate();
                if (result == 0) {
                    throw new RuntimeException("Error!! Cannot insert into LineItem for order_id =" + order_id);
                }
            
                // get returned keys: line item id
                int line_id = 0;
                try (ResultSet generatedKeys = stmtInsertLineItem.getGeneratedKeys()) {
                    if (!generatedKeys.next()) {
                        throw new RuntimeException("stmtInsertLineItem generatedKeys failed!");
                    }
                    line_id = generatedKeys.getInt(1);
                }
                if (line_id == 0) {
                    LOG.warn("new line item not inserted for order_id = " + order_id);
                }

                //update the line item
                double pre_tax_amount = price;
                stmtUpdateLineItem.setDouble(1, pre_tax_amount);
                stmtUpdateLineItem.setInt(2, line_id);
                result = stmtUpdateLineItem.executeUpdate();
                if (result == 0) {
                    throw new RuntimeException("Error!! Cannot update on LineItem for line_id =" + line_id);
                }

                //get line_item quantity sum for the order
                stmtSumLineItemQuantity.setInt(1, order_id);
                int totalQuantity = 0;
                try (ResultSet rs = stmtSumLineItemQuantity.executeQuery()) {
                    if (rs.next()) {
                        totalQuantity = rs.getInt(1);
                    }
                }

                //get lint_item total for the order
                stmtSumLineItemTotal.setInt(1, order_id);
                double totalAmount = 0.0;
                try (ResultSet rs = stmtSumLineItemTotal.executeQuery()) {
                    if (rs.next()) {
                        totalAmount = rs.getDouble(1);
                    }
                }

                //update order details
                stmtUpdateOrderDetails.setDouble(1, totalAmount); // item_total
                stmtUpdateOrderDetails.setInt(2, totalQuantity);  //item_count
                stmtUpdateOrderDetails.setDouble(3, totalAmount); // total
                stmtUpdateOrderDetails.setTimestamp(4, sysdate);  //updated_at
                stmtUpdateOrderDetails.setInt(5, order_id);
                result = stmtUpdateOrderDetails.executeUpdate();
                if (result == 0) {
                    throw new RuntimeException("Error!! Cannot update order details for order_id = " + order_id);
                }

                //update order timestamp
                stmtUpdateOrder.setTimestamp(1, sysdate);
                stmtUpdateOrder.setInt(2, order_id);
                result = stmtUpdateOrder.executeUpdate();
                if (result == 0) {
                    throw new RuntimeException("Error!! Cannot update updated_at on Order for id =" + order_id);
                }
        }

    }

}




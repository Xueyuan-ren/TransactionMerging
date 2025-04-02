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

package com.grpc.examples.spree;

import com.grpc.examples.Procedure;
import com.grpc.examples.SpreeAddItemRequest;
import com.grpc.examples.SpreeAddItemReply;
import com.grpc.examples.api.SQLStmt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SpreeAddItemMT extends Procedure {

    private static final Logger LOG = LoggerFactory.getLogger(SpreeAddItemMT.class);

    private SpreeAddItemReply.Builder response = SpreeAddItemReply.newBuilder();

    private int MERGE_SIZE;

    public SpreeAddItemMT(int merge) {
        MERGE_SIZE = merge;
    }

    public SpreeAddItemReply spreeAddItemMergeTransaction(SpreeAddItemRequest[] requests, Connection conn) throws SQLException {

        // set parameters
        double defprice = -1.0;
        int[] variant_ids = new int[MERGE_SIZE];
        int[] product_ids = new int[MERGE_SIZE];
        String currency = requests[0].getCurrency();
        int[] order_ids = new int[MERGE_SIZE];
        int[] quantitys = new int[MERGE_SIZE];
        Map<Integer, Double> variant_price = new HashMap<>();
        Map<Integer, Integer> order_quantity = new HashMap<>();
        Map<Integer, Double> order_itemTotal = new HashMap<>();

        for (int i = 0; i < MERGE_SIZE; i++) {
            int variant_id = requests[i].getVariantId();
            int product_id = requests[i].getProductId();
            int order_id = requests[i].getOrderId();
            int quantity = requests[i].getQuantity();
            variant_price.put(variant_id, defprice);
            variant_ids[i] = variant_id;
            product_ids[i] = product_id;
            order_ids[i] = order_id;
            quantitys[i] = quantity;
        }
        // non-duplicate variants/products count
        int nondup = variant_price.size();

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
        for (int i = 0; i < MERGE_SIZE - 1; i++) {
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
        for (int i = 0; i < MERGE_SIZE; i++) {
            stmtUpdateLineItemMTSQL += "WHEN spree_line_items.id = ? THEN ? ";
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateLineItemMTSQL += "ELSE spree_line_items.pre_tax_amount END WHERE spree_line_items.id IN (";
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.item_total END, spree_orders.item_count = CASE ";
        for (int i = 0; i < MERGE_SIZE; i++) {
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.item_count END, spree_orders.total = CASE ";
        for (int i = 0; i < MERGE_SIZE; i++) {
            stmtUpdateOrderDetailsMTSQL += "WHEN spree_orders.id = ? THEN ? ";
        }
        stmtUpdateOrderDetailsMTSQL += "ELSE spree_orders.total END, spree_orders.updated_at = ? WHERE spree_orders.id IN (";
        for (int i = 0; i < MERGE_SIZE - 1; i++) {
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
            PreparedStatement stmtInsertLineItem = this.getPreparedStatementReturnKeys(conn, stmtInsertLineItemMT, new int[]{MERGE_SIZE});
            PreparedStatement stmtUpdateLineItem = this.getPreparedStatement(conn, stmtUpdateLineItemMT);
            PreparedStatement stmtSumLineItemQuantity = this.getPreparedStatement(conn, stmtSumLineItemQuantityMT);
            PreparedStatement stmtSumLineItemTotal = this.getPreparedStatement(conn, stmtSumLineItemTotalMT);
            PreparedStatement stmtUpdateOrderDetails = this.getPreparedStatement(conn, stmtUpdateOrderDetailsMT);
            PreparedStatement stmtUpdateOrder = this.getPreparedStatement(conn, stmtUpdateOrderMT)) {

            //load price
            //TODO: check spree_prices index, maybe using (variant_id, currency) is more efficient
            stmtLoadPrice.setString(1, currency);
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtLoadPrice.setInt(i + 2, variant_ids[i]);
            }
            int priceCount = 0;
            try (ResultSet rs = stmtLoadPrice.executeQuery()) {
                while (rs.next()) {
                    double price = rs.getDouble("amount");
                    int variant_id = rs.getInt("variant_id");
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
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtLoadLineItem.setInt(i*2 + 1, order_ids[i]);
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
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtLoadTaxCategory.setInt(i + 1, tax_category_id);
            }
            try (ResultSet rs = stmtLoadTaxCategory.executeQuery()) {
                // if the Tax Category not exist
                if (!rs.next()) {
                    throw new RuntimeException("The tax_category_id " + tax_category_id + " not exist!");
                }
            }

            //load product
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtLoadProduct.setInt(i + 1, product_ids[i]);
            }
            int productCount = 0;
            try (ResultSet rs = stmtLoadProduct.executeQuery()) { 
                while (rs.next()) {
                    productCount++;
                }
            }
            // check if the product not exist
            if (productCount < nondup) {
                throw new RuntimeException("Has a product_id not exist!");
            }

            //check stock
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtCheckStock.setInt(i + 1, variant_ids[i]);
            }
            int stockItem = 0;
            try (ResultSet rs = stmtCheckStock.executeQuery()) { 
                while (rs.next()) {
                    stockItem++;
                }
            }
            // if the stock item not exists
            if (stockItem < nondup) {
                throw new RuntimeException("Has a stock variant_id not exist!");
            }

            //insert order line item
            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtInsertLineItem.setInt(i*7 + 1, variant_ids[i]);
                stmtInsertLineItem.setInt(i*7 + 2, order_ids[i]);
                stmtInsertLineItem.setInt(i*7 + 3, quantitys[i]);
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
            ArrayList<Integer> line_ids = new ArrayList<>(MERGE_SIZE);
            try (ResultSet generatedKeys = stmtInsertLineItem.getGeneratedKeys()) {
                while (generatedKeys.next()) {
                    line_ids.add(generatedKeys.getInt(1));
                }   
            }
            if (line_ids.size() != MERGE_SIZE) {
                throw new RuntimeException("Some stmtInsertLineItem getting line_id failed!");
            }

            //update the line_item
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtUpdateLineItem.setInt(i*2 + 1, line_ids.get(i));
                stmtUpdateLineItem.setDouble(i*2 + 2, variant_price.get(variant_ids[i]));
                stmtUpdateLineItem.setInt(MERGE_SIZE*2 + i + 1 , line_ids.get(i));
            }
            result = stmtUpdateLineItem.executeUpdate();
            if (result == 0) {
                LOG.error("update spree_line_items failed");
                throw new RuntimeException("Error: Cannot update spree_line_items!");
            }

            //get quantity sum for this line_item
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtSumLineItemQuantity.setInt(i + 1, order_ids[i]);
            }
            try (ResultSet rs = stmtSumLineItemQuantity.executeQuery()) {
                while (rs.next()) {;
                    order_quantity.put(rs.getInt("order_id"), rs.getInt("sum_quantity"));
                }
            }
            if (order_quantity.size() != MERGE_SIZE) {
                // System.out.println("order_ids:" + Arrays.toString(order_ids));
                // System.out.println("order_quantity:" + order_quantity.toString());
                // has duplicate order_ids
                //throw new RuntimeException("Some stmtSumLineItemQuantity getting sum_quantity failed!");
            }

            //get total for this line_item
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtSumLineItemTotal.setInt(i + 1, order_ids[i]);
            }
            try (ResultSet rs = stmtSumLineItemTotal.executeQuery()) {
                while (rs.next()) {
                    order_itemTotal.put(rs.getInt("order_id"), rs.getDouble("total"));
                }
            }
            if (order_itemTotal.size() != MERGE_SIZE) {
                // has duplicate order_ids
                //throw new RuntimeException("Some stmtSumLineItemTotal getting total failed!");
            }

            //update order details
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtUpdateOrderDetails.setInt(i*2 + 1, order_ids[i]); // order_id
                stmtUpdateOrderDetails.setDouble(i*2 + 2, order_itemTotal.get(order_ids[i])); // item_total
                
                stmtUpdateOrderDetails.setInt(MERGE_SIZE*2 + i*2 + 1, order_ids[i]); // order_id
                stmtUpdateOrderDetails.setInt(MERGE_SIZE*2 + i*2 + 2, order_quantity.get(order_ids[i]));  //item_count

                stmtUpdateOrderDetails.setInt(MERGE_SIZE*2 + MERGE_SIZE*2 + i*2 + 1, order_ids[i]); // order_id
                stmtUpdateOrderDetails.setDouble(MERGE_SIZE*2 + MERGE_SIZE*2 + i*2 + 2, order_itemTotal.get(order_ids[i])); // total
                
                stmtUpdateOrderDetails.setInt(MERGE_SIZE*2 + MERGE_SIZE*2 + MERGE_SIZE*2 + i + 2, order_ids[i]);
            }
            stmtUpdateOrderDetails.setTimestamp(MERGE_SIZE*2 + MERGE_SIZE*2 + MERGE_SIZE*2 + 1, sysdate);  //updated_at
            result = stmtUpdateOrderDetails.executeUpdate();
            if (result == 0) {
                LOG.error("update spree_orders failed");
                throw new RuntimeException("Error: Cannot update order details!");
            }

            //update order timestamp
            stmtUpdateOrder.setTimestamp(1, sysdate);
            for (int i = 0; i < MERGE_SIZE; i++) {
                stmtUpdateOrder.setInt(i + 2, order_ids[i]);
            }
            result = stmtUpdateOrder.executeUpdate();
            if (result == 0) {
                LOG.error("update spree_orders timestamps failed");
                throw new RuntimeException("Error: Cannot update updated_at on spree_order!");
            }
        }

        response.setCompleted(true);
        return response.build();
    }
}




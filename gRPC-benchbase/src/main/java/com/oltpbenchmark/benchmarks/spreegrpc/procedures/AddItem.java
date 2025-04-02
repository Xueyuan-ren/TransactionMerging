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

package com.oltpbenchmark.benchmarks.spreegrpc.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeConfig;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeUtil;
import com.oltpbenchmark.benchmarks.spreegrpc.SpreeWorker;
import com.oltpbenchmark.grpcservice.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Random;

import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;

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


    public void run(Connection conn, ManagedChannel channel, TxnServiceGrpc.TxnServiceBlockingStub blockingStub,
                    Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, SpreeWorker w) 
                    throws SQLException, StatusRuntimeException {

        int numItems = SpreeConfig.configItemCount;
        int total_orders = (int) SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;
        int variant_id = SpreeUtil.randomNumber(1, numItems, gen);
        String currency = "USD";
        int order_id = SpreeUtil.randomNumber(1, total_orders, gen);
        int product_id = variant_id;
        int quantity = SpreeUtil.randomNumber(1, 10, gen);
        int workid = w.getId();

        SpreeAddItemRequest request = SpreeAddItemRequest.newBuilder()
                                        .setVariantId(variant_id)
                                        .setProductId(product_id)
                                        .setCurrency(currency)
                                        .setOrderId(order_id)
                                        .setQuantity(quantity)
                                        .setWorkid(workid)
                                        .build();

        SpreeAddItemReply response;
        response = blockingStub.spreeAddItemTxn(request);

    }

}




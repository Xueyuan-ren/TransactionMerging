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


package com.oltpbenchmark.benchmarks.spreenorpc;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.benchmarks.spreenorpc.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Spree Loader
 */
public class SpreeLoader extends Loader<SpreenorpcBenchmark> {

    private final long numWarehouses;

    private final Random rand = new Random();

    public SpreeLoader(SpreenorpcBenchmark benchmark) {
        super(benchmark);
        numWarehouses = Math.max(Math.round(SpreeConfig.configWhseCount * this.scaleFactor), 1);
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        int total_users = SpreeConfig.configCustPerWhse * SpreeConfig.configWhseCount;

        // Load tables by different threads
        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //PRODUCT
                loadProducts(conn, SpreeConfig.configItemCount);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //PRICE
                loadPrices(conn, SpreeConfig.configItemCount);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //CUSTOMER
                loadCustomers(conn, total_users);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //ORDER
                loadOpenOrders(conn, total_users);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //ORDER_LINES
                loadOrderline(conn, total_users);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //STOCK Location
                loadStockLocation(conn, numWarehouses);
                //Tax Category
                loadTaxCategories(conn);
            }

        });

        threads.add(new LoaderThread(this.benchmark) {
            @Override
            public void load(Connection conn) {
                //STOCK_ITEMS: 100000 * numWarehouses
                loadStock(conn, SpreeConfig.configItemCount, numWarehouses);
            }

        });
        
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        return conn.prepareStatement(sql);
    }

    protected void loadProducts(Connection conn, int itemCount) {
        
        try (PreparedStatement itemPrepStmt = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREEPRODUCTS)) {
            int batchSize = 0;
            for (int i = 1; i <= itemCount; i++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                SpreeProduct item = new SpreeProduct();
                item.id = i;
                item.name = SpreeUtil.randomStr(SpreeUtil.randomNumber(14, 24, benchmark.rng()));
                //item.i_im_id = TPCCUtil.randomNumber(1, 10000, benchmark.rng());

                int idx = 1;
                int tax_category_id = 1;
                int shipping_category_id = 1;
                int promotionable = 1;
                String status = "active";
                item.created_at = sysdate;
                item.updated_at = sysdate;
                item.available_on = sysdate;
                item.make_active_at = sysdate;

                itemPrepStmt.setLong(idx++, item.id);
                itemPrepStmt.setString(idx++, item.name);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setTimestamp(idx++, item.available_on);
                itemPrepStmt.setNull(idx++, Types.TIMESTAMP);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setLong(idx++, tax_category_id);
                itemPrepStmt.setLong(idx++, shipping_category_id);
                itemPrepStmt.setTimestamp(idx++,  item.created_at);
                itemPrepStmt.setTimestamp(idx++, item.updated_at);
                itemPrepStmt.setLong(idx++, promotionable);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setNull(idx++, Types.TIMESTAMP);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setNull(idx++, Types.VARCHAR);
                itemPrepStmt.setString(idx++, status);
                itemPrepStmt.setTimestamp(idx, item.make_active_at);
                itemPrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getBatchSize()) {
                    itemPrepStmt.executeBatch();
                    itemPrepStmt.clearBatch();
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                itemPrepStmt.executeBatch();
                itemPrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
    }

    protected void loadPrices(Connection conn, int variantCount) {
        
        try (PreparedStatement pricePrepStmt = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREEPRICES)) {
            int batchSize = 0;
            for (int i = 1; i <= variantCount; i++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                SpreePrice price = new SpreePrice();
                price.id = i;
                price.variant_id = i;
                price.amount =  SpreeUtil.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                int idx = 1;
                String currency = "USD";
                price.created_at = sysdate;
                price.updated_at = sysdate;

                pricePrepStmt.setLong(idx++, price.id);
                pricePrepStmt.setLong(idx++, price.variant_id);
                pricePrepStmt.setDouble(idx++, price.amount);
                pricePrepStmt.setString(idx++, currency);
                pricePrepStmt.setNull(idx++, Types.TIMESTAMP);
                pricePrepStmt.setTimestamp(idx++,  price.created_at);
                pricePrepStmt.setTimestamp(idx++, price.updated_at);
                pricePrepStmt.setNull(idx, Types.DOUBLE);
                pricePrepStmt.addBatch();
                batchSize++;

                if (batchSize == workConf.getBatchSize()) {
                    pricePrepStmt.executeBatch();
                    pricePrepStmt.clearBatch();
                    batchSize = 0;
                }
            }

            if (batchSize > 0) {
                pricePrepStmt.executeBatch();
                pricePrepStmt.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
    }

    protected void loadCustomers(Connection conn, int total_users) {

        int k = 0;

        //StringBuilder sb = new StringBuilder();
        //sb.append("INSERT INTO spree_users ()")

        try (PreparedStatement custPrepStmt = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREEUSERS)) {
            
            for (int c = 1; c <= total_users; c++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                SpreeUser customer = new SpreeUser();
                customer.id = c;
                customer.email = "spree." + c + "@example.com";
                customer.created_at = sysdate;
                customer.updated_at = sysdate;

                int idx = 1;
                int def = 0;

                custPrepStmt.setLong(idx++, customer.id);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setString(idx++, customer.email);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setInt(idx++, def);
                custPrepStmt.setInt(idx++, def);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.BIGINT);
                custPrepStmt.setNull(idx++, Types.BIGINT);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setTimestamp(idx++, customer.created_at);
                custPrepStmt.setTimestamp(idx++, customer.updated_at);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx++, Types.VARCHAR);
                custPrepStmt.setNull(idx++, Types.TIMESTAMP);
                custPrepStmt.setNull(idx, Types.TIMESTAMP);
                custPrepStmt.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                    custPrepStmt.executeBatch();
                    custPrepStmt.clearBatch();
                }
            }

            custPrepStmt.executeBatch();
            custPrepStmt.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected void loadTaxCategories(Connection conn) {

        try (PreparedStatement TaxGategoryPrepStmt = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREETAXCATEGORIES)) {

            Timestamp sysdate = new Timestamp(System.currentTimeMillis());
            SpreeTaxCategory taxcat = new SpreeTaxCategory();
            taxcat.id = 1;
            taxcat.name = "Clothing";
            taxcat.is_default = 0;
            taxcat.created_at = sysdate;
            taxcat.updated_at = sysdate;
                
            int idx = 1;
            TaxGategoryPrepStmt.setLong(idx++, taxcat.id);
            TaxGategoryPrepStmt.setString(idx++, taxcat.name);
            TaxGategoryPrepStmt.setNull(idx++, Types.VARCHAR);
            TaxGategoryPrepStmt.setInt(idx++, taxcat.is_default);
            TaxGategoryPrepStmt.setNull(idx++, Types.TIMESTAMP);
            TaxGategoryPrepStmt.setTimestamp(idx++,  taxcat.created_at);
            TaxGategoryPrepStmt.setTimestamp(idx++, taxcat.updated_at);
            TaxGategoryPrepStmt.setNull(idx, Types.VARCHAR);
            TaxGategoryPrepStmt.execute();
                
        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
    }

    protected void loadOpenOrders(Connection conn, int total_users) {

        int k = 0;

        try (PreparedStatement openOrderStatement = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREEORDERS)) {

            for (int c = 1; c <= total_users; c++) {

                Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                SpreeOrder oorder = new SpreeOrder();
                oorder.id = k + 1;
                oorder.email = "spree." + c + "@example.com";
                oorder.user_id = c;
                oorder.state = "cart";
                oorder.created_at = sysdate;
                oorder.updated_at = sysdate;
                oorder.store_id = 1;
                String num = "R" + SpreeUtil.randomNStr(9);
                oorder.number = num;

                int idx = 1;
                int defint = 0;
                double defdec = 0.00;
                String channel = "spree";

                openOrderStatement.setLong(idx++, oorder.id);
                openOrderStatement.setString(idx++, oorder.number);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setNull(idx++, Types.TIMESTAMP);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setTimestamp(idx++, oorder.created_at);
                openOrderStatement.setTimestamp(idx++, oorder.updated_at);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setString(idx++, channel);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setInt(idx++, defint);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setNull(idx++, Types.TIMESTAMP);
                openOrderStatement.setNull(idx++, Types.TINYINT);
                openOrderStatement.setNull(idx++, Types.TINYINT);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.TIMESTAMP);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setNull(idx++, Types.BIGINT);
                openOrderStatement.setInt(idx++, defint);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setDouble(idx++, defdec);
                openOrderStatement.setNull(idx++, Types.TINYINT);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.setNull(idx++, Types.VARCHAR);
                openOrderStatement.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                    openOrderStatement.executeBatch();
                    openOrderStatement.clearBatch();
                }

            }

            openOrderStatement.executeBatch();
            openOrderStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    protected void loadOrderline(Connection conn, int total_users) {

        int k = 0;

        try (PreparedStatement orderLineStatement = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREEORDERLINES)) {

            for (int c = 1; c <= total_users; c++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                SpreeLineitem lineitem = new SpreeLineitem();
                lineitem.id = k + 1;
                lineitem.variant_id = SpreeUtil.randomNumber(1, SpreeConfig.configItemCount, rand);
                lineitem.order_id = c;
                lineitem.quantity = 1;
                lineitem.price = 1.0f;
                lineitem.created_at = sysdate;
                lineitem.updated_at = sysdate;

                int idx = 1;
                double defdec = 0.00;
                String currency = "USD";
                
                orderLineStatement.setLong(idx++, lineitem.id);
                orderLineStatement.setLong(idx++, lineitem.variant_id);
                orderLineStatement.setLong(idx++, lineitem.order_id);
                orderLineStatement.setInt(idx++, lineitem.quantity);
                orderLineStatement.setDouble(idx++, lineitem.price);
                orderLineStatement.setTimestamp(idx++, lineitem.created_at);
                orderLineStatement.setTimestamp(idx++, lineitem.updated_at);
                orderLineStatement.setString(idx++, currency);
                orderLineStatement.setNull(idx++, Types.DOUBLE);
                orderLineStatement.setNull(idx++, Types.BIGINT);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setDouble(idx++, defdec);
                orderLineStatement.setNull(idx++, Types.VARCHAR);
                orderLineStatement.setNull(idx++, Types.VARCHAR);
                orderLineStatement.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        orderLineStatement.executeBatch();
                        orderLineStatement.clearBatch();
                }
            }

            orderLineStatement.executeBatch();
            orderLineStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    protected void loadStock(Connection conn, int numItems, long numWarehouses) {

        int k = 0;
        int locs = (int) numWarehouses;

        try (PreparedStatement stockPreparedStatement = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREESTOCK)) {

            for (int loc = 1; loc <= locs; loc++) {
                for (int c = 1; c <= numItems; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                    SpreeStock stock = new SpreeStock();
                    stock.id = c + numItems * (loc - 1);
                    stock.stock_location_id = loc;
                    stock.variant_id = c;
                    stock.created_at = sysdate;
                    stock.updated_at = sysdate;
                    stock.count_on_hand = SpreeUtil.randomNumber(10, 100, rand);

                    int idx = 1;
                    int defint = 0;
                    stockPreparedStatement.setLong(idx++, stock.id);
                    stockPreparedStatement.setLong(idx++, stock.stock_location_id);
                    stockPreparedStatement.setLong(idx++, stock.variant_id);
                    stockPreparedStatement.setInt(idx++, stock.count_on_hand);
                    stockPreparedStatement.setTimestamp(idx++, stock.created_at);
                    stockPreparedStatement.setTimestamp(idx++, stock.updated_at);
                    stockPreparedStatement.setInt(idx++, defint);
                    stockPreparedStatement.setNull(idx++, Types.TIMESTAMP);
                    stockPreparedStatement.setNull(idx++, Types.VARCHAR);
                    stockPreparedStatement.setNull(idx++, Types.VARCHAR);
                    stockPreparedStatement.addBatch();

                    k++;

                    if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                        stockPreparedStatement.executeBatch();
                        stockPreparedStatement.clearBatch();
                    }

                }
                stockPreparedStatement.executeBatch();
                stockPreparedStatement.clearBatch();
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

    protected void loadStockLocation(Connection conn, long numWarehouses) {

        int k = 0;
        int locs = (int) numWarehouses;

        try (PreparedStatement stockLocsStatement = getInsertStatement(conn, SpreeConstants.TABLENAME_SPREESTOCKLOCATIONS)) {

            for (int c = 1; c <= locs; c++) {

                Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                SpreeStocklocation stockloc = new SpreeStocklocation();
                stockloc.id = c;
                stockloc.name =  "stockloc" + c;
                stockloc.created_at = sysdate;
                stockloc.updated_at = sysdate;
                stockloc.active = 1;
                stockloc.backorderable_default = 0;
                stockloc.propagate_all_variants = 0;

                int idx = 1;
                stockLocsStatement.setLong(idx++, stockloc.id);
                stockLocsStatement.setString(idx++, stockloc.name);
                stockLocsStatement.setTimestamp(idx++, stockloc.created_at);
                stockLocsStatement.setTimestamp(idx++, stockloc.updated_at);
                stockLocsStatement.setInt(idx++, 0);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx++, Types.BIGINT);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx++, Types.BIGINT);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setInt(idx++, stockloc.active);
                stockLocsStatement.setInt(idx++, stockloc.backorderable_default);
                stockLocsStatement.setInt(idx++, stockloc.propagate_all_variants);
                stockLocsStatement.setNull(idx++, Types.VARCHAR);
                stockLocsStatement.setNull(idx, Types.TIMESTAMP);
                stockLocsStatement.addBatch();

                k++;

                if (k != 0 && (k % workConf.getBatchSize()) == 0) {
                    stockLocsStatement.executeBatch();
                    stockLocsStatement.clearBatch();
                }

            }

            stockLocsStatement.executeBatch();
            stockLocsStatement.clearBatch();

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }

    }

}

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
package com.oltpbenchmark.benchmarks.tpccHistories;

import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.apiHistory.LoaderThreadHistory;
import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.*;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.SQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
/**
 * TPC-C Benchmark Loader
 */
public class TPCCLoaderHistory extends Loader<TPCCBenchmarkHistory> {

    private static final int FIRST_UNPROCESSED_O_ID = 2101;

    private final long numWarehouses;

    private int so;

    public TPCCLoaderHistory(TPCCBenchmarkHistory benchmark, int so) {
        super(benchmark);
        numWarehouses = Math.max(Math.round(TPCCConfigHistory.configWhseCount * this.scaleFactor), 1);
        this.so = so;
    }

    @Override
    public List<LoaderThread> createLoaderThreads() {
        List<LoaderThread> threads = new ArrayList<>();
        final CountDownLatch itemLatch = new CountDownLatch(1);

        // ITEM
        // This will be invoked first and executed in a single thread.
        threads.add(new LoaderThreadHistory(this.benchmark) {
            @Override
            public void load(Connection conn) {
                loadItems(conn, TPCCConfigHistory.configItemCount, events, so, 0);
            }

            @Override
            public void afterLoad() {
                itemLatch.countDown();
            }
        });

        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        for (int w = 1; w <= numWarehouses; w++) {
            final int w_id = w;
            LoaderThreadHistory t = new LoaderThreadHistory(this.benchmark) {
                @Override
                public void load(Connection conn) {

                    Integer po = 0;

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load WAREHOUSE {}", w_id);
                    }
                    // WAREHOUSE
                    loadWarehouse(conn, w_id, events, so+w_id, po);
                    ++po;

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load STOCK {}", w_id);
                    }
                    // STOCK
                    po = loadStock(conn, w_id, TPCCConfigHistory.configItemCount, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load DISTRICT {}", w_id);
                    }
                    // DISTRICT
                    po = loadDistricts(conn, w_id, TPCCConfigHistory.configDistPerWhse, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load CUSTOMER {}", w_id);
                    }
                    // CUSTOMER
                    po = loadCustomers(conn, w_id, TPCCConfigHistory.configDistPerWhse, TPCCConfigHistory.configCustPerDist, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load CUSTOMER HISTORY {}", w_id);
                    }
                    // CUSTOMER HISTORY
                    po = loadCustomerHistory(conn, w_id, TPCCConfigHistory.configDistPerWhse, TPCCConfigHistory.configCustPerDist, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load ORDERS {}", w_id);
                    }
                    // ORDERS
                    po = loadOpenOrders(conn, w_id, TPCCConfigHistory.configDistPerWhse, TPCCConfigHistory.configCustPerDist, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load NEW ORDERS {}", w_id);
                    }
                    // NEW ORDERS
                    po = loadNewOrders(conn, w_id, TPCCConfigHistory.configDistPerWhse, TPCCConfigHistory.configCustPerDist, events, so+w_id, po);

                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Starting to load ORDER LINES {}", w_id);
                    }
                    // ORDER LINES
                    po = loadOrderLines(conn, w_id, TPCCConfigHistory.configDistPerWhse, TPCCConfigHistory.configCustPerDist, events, so+w_id, po);

                }

                @Override
                public void beforeLoad() {

                    // Make sure that we load the ITEM table first

                    try {
                        itemLatch.await();
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            };
            threads.add(t);
        }
        return (threads);
    }

    private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
        Table catalog_tbl = benchmark.getCatalog().getTable(tableName);
        String sql = SQLUtil.getInsertSQL(catalog_tbl, this.getDatabaseType());
        sql = sql + " RETURNING *";
        return conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }


    protected void loadItems(Connection conn, int itemCount, ArrayList<Event> events, Integer so, Integer po) {

        try (PreparedStatement itemPrepStmt = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_ITEM)) {

            for (int i = 1; i <= itemCount; i++) {

                ItemHistory item = new ItemHistory();
                item.i_id = i;
                item.i_name = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(14, 24, benchmark.rng()));
                item.i_price = TPCCUtilHistory.randomNumber(100, 10000, benchmark.rng()) / 100.0;

                // i_data
                int randPct = TPCCUtilHistory.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtilHistory.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 .. 50]
                    item.i_data = TPCCUtilHistory.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere in
                    // middle
                    int startORIGINAL = TPCCUtilHistory.randomNumber(2, (len - 8), benchmark.rng());
                    item.i_data = TPCCUtilHistory.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtilHistory.randomStr(len - startORIGINAL - 9);
                }

                item.i_im_id = TPCCUtilHistory.randomNumber(1, 10000, benchmark.rng());
                item.i_writeID = EventID.generateID(0, so, po);


                int idx = 1;
                itemPrepStmt.setLong(idx++, item.i_id);
                itemPrepStmt.setString(idx++, item.i_name);
                itemPrepStmt.setDouble(idx++, item.i_price);
                itemPrepStmt.setString(idx++, item.i_data);
                itemPrepStmt.setLong(idx++, item.i_im_id);
                itemPrepStmt.setString(idx, item.i_writeID);

                itemPrepStmt.execute();
                var rs = itemPrepStmt.getResultSet();
                var p = item.getInsertEventInfo(rs);
                events.add(new InsertEvent(0, so, po, p, item.getTableNames()));
                ++po;

            }


        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }


    protected void loadWarehouse(Connection conn, int w_id, ArrayList<Event> events, Integer so, Integer po) {

        try (PreparedStatement whsePrepStmt = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_WAREHOUSE)) {
            WarehouseHistory warehouse = new WarehouseHistory();

            warehouse.w_id = w_id;
            warehouse.w_ytd = 300000;

            // random within [0.0000 .. 0.2000]
            warehouse.w_tax = (TPCCUtilHistory.randomNumber(0, 2000, benchmark.rng())) / 10000.0;
            warehouse.w_name = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(6, 10, benchmark.rng()));
            warehouse.w_street_1 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_street_2 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_city = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
            warehouse.w_state = TPCCUtilHistory.randomStr(3).toUpperCase();
            warehouse.w_zip = "123456789";
            warehouse.w_writeID = EventID.generateID(0, so, po);

            int idx = 1;
            whsePrepStmt.setLong(idx++, warehouse.w_id);
            whsePrepStmt.setDouble(idx++, warehouse.w_ytd);
            whsePrepStmt.setDouble(idx++, warehouse.w_tax);
            whsePrepStmt.setString(idx++, warehouse.w_name);
            whsePrepStmt.setString(idx++, warehouse.w_street_1);
            whsePrepStmt.setString(idx++, warehouse.w_street_2);
            whsePrepStmt.setString(idx++, warehouse.w_city);
            whsePrepStmt.setString(idx++, warehouse.w_state);
            whsePrepStmt.setString(idx++, warehouse.w_zip);
            whsePrepStmt.setString(idx, warehouse.w_writeID);

            whsePrepStmt.execute();

            var rs = whsePrepStmt.getResultSet();
            var p = warehouse.getInsertEventInfo(rs);
            events.add(new InsertEvent(0, so, po, p, warehouse.getTableNames()));

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }

    }

    protected int loadStock(Connection conn, int w_id, int numItems, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement stockPreparedStatement = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_STOCK)) {

            for (int i = 1; i <= numItems; i++) {
                StockHistory stock = new StockHistory();
                stock.s_i_id = i;
                stock.s_w_id = w_id;
                stock.s_quantity = TPCCUtilHistory.randomNumber(10, 100, benchmark.rng());
                stock.s_ytd = 0;
                stock.s_order_cnt = 0;
                stock.s_remote_cnt = 0;
                stock.s_writeID = EventID.generateID(0, so, po);

                // s_data
                int randPct = TPCCUtilHistory.randomNumber(1, 100, benchmark.rng());
                int len = TPCCUtilHistory.randomNumber(26, 50, benchmark.rng());
                if (randPct > 10) {
                    // 90% of time i_data isa random string of length [26 ..
                    // 50]
                    stock.s_data = TPCCUtilHistory.randomStr(len);
                } else {
                    // 10% of time i_data has "ORIGINAL" crammed somewhere
                    // in middle
                    int startORIGINAL = TPCCUtilHistory.randomNumber(2, (len - 8), benchmark.rng());
                    stock.s_data = TPCCUtilHistory.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtilHistory.randomStr(len - startORIGINAL - 9);
                }

                int idx = 1;
                stockPreparedStatement.setLong(idx++, stock.s_w_id);
                stockPreparedStatement.setLong(idx++, stock.s_i_id);
                stockPreparedStatement.setLong(idx++, stock.s_quantity);
                stockPreparedStatement.setDouble(idx++, stock.s_ytd);
                stockPreparedStatement.setLong(idx++, stock.s_order_cnt);
                stockPreparedStatement.setLong(idx++, stock.s_remote_cnt);
                stockPreparedStatement.setString(idx++, stock.s_data);
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx++, TPCCUtilHistory.randomStr(24));
                stockPreparedStatement.setString(idx, stock.s_writeID);

                stockPreparedStatement.execute();

                var rs = stockPreparedStatement.getResultSet();
                var p = stock.getInsertEventInfo(rs);
                events.add(new InsertEvent(0, so, po, p, stock.getTableNames()));
                ++po;

            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
        return po;
    }

    protected int loadDistricts(Connection conn, int w_id, int districtsPerWarehouse, ArrayList<Event> events, Integer so, Integer po) {

        try (PreparedStatement distPrepStmt = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_DISTRICT)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                DistrictHistory district = new DistrictHistory();
                district.d_id = d;
                district.d_w_id = w_id;
                district.d_ytd = 30000;

                // random within [0.0000 .. 0.2000]
                district.d_tax = (float) ((TPCCUtilHistory.randomNumber(0, 2000, benchmark.rng())) / 10000.0);

                district.d_next_o_id = TPCCConfigHistory.configCustPerDist + 1;
                district.d_name = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(6, 10, benchmark.rng()));
                district.d_street_1 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                district.d_street_2 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                district.d_city = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                district.d_state = TPCCUtilHistory.randomStr(3).toUpperCase();
                district.d_zip = "123456789";
                district.d_writeID = EventID.generateID(0, so, po);

                int idx = 1;
                distPrepStmt.setLong(idx++, district.d_w_id);
                distPrepStmt.setLong(idx++, district.d_id);
                distPrepStmt.setDouble(idx++, district.d_ytd);
                distPrepStmt.setDouble(idx++, district.d_tax);
                distPrepStmt.setLong(idx++, district.d_next_o_id);
                distPrepStmt.setString(idx++, district.d_name);
                distPrepStmt.setString(idx++, district.d_street_1);
                distPrepStmt.setString(idx++, district.d_street_2);
                distPrepStmt.setString(idx++, district.d_city);
                distPrepStmt.setString(idx++, district.d_state);
                distPrepStmt.setString(idx++, district.d_zip);
                distPrepStmt.setString(idx, district.d_writeID);
                distPrepStmt.execute();

                var rs = distPrepStmt.getResultSet();
                var p = district.getInsertEventInfo(rs);
                events.add(new InsertEvent(0, so, po, p, district.getTableNames()));
                ++po;
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
        return po;

    }

    protected int loadCustomers(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement custPrepStmt = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_CUSTOMER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                    CustomerHistory customer = new CustomerHistory();
                    customer.c_id = c;
                    customer.c_d_id = d;
                    customer.c_w_id = w_id;

                    // discount is random between [0.0000 ... 0.5000]
                    customer.c_discount = (float) (TPCCUtilHistory.randomNumber(1, 5000, benchmark.rng()) / 10000.0);

                    if (TPCCUtilHistory.randomNumber(1, 100, benchmark.rng()) <= 10) {
                        customer.c_credit = "BC"; // 10% Bad Credit
                    } else {
                        customer.c_credit = "GC"; // 90% Good Credit
                    }
                    if (c <= TPCCConfigHistory.configMaxDistinctCust) {
                        customer.c_last = TPCCUtilHistory.getLastName(c - 1);
                    } else {
                        customer.c_last = TPCCUtilHistory.getNonUniformRandomLastNameForLoad(benchmark.rng());
                    }
                    customer.c_first = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(8, 16, benchmark.rng()));
                    customer.c_credit_lim = 50000;

                    customer.c_balance = -10;
                    customer.c_ytd_payment = 10;
                    customer.c_payment_cnt = 1;
                    customer.c_delivery_cnt = 0;

                    customer.c_street_1 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                    customer.c_street_2 = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                    customer.c_city = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 20, benchmark.rng()));
                    customer.c_state = TPCCUtilHistory.randomStr(3).toUpperCase();
                    // TPC-C 4.3.2.7: 4 random digits + "11111"
                    customer.c_zip = TPCCUtilHistory.randomNStr(4) + "11111";
                    customer.c_phone = TPCCUtilHistory.randomNStr(16);
                    customer.c_since = sysdate;
                    customer.c_middle = "OE";
                    customer.c_data = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(300, 500, benchmark.rng()));
                    customer.c_writeID = EventID.generateID(0, so, po);

                    int idx = 1;
                    custPrepStmt.setLong(idx++, customer.c_w_id);
                    custPrepStmt.setLong(idx++, customer.c_d_id);
                    custPrepStmt.setLong(idx++, customer.c_id);
                    custPrepStmt.setDouble(idx++, customer.c_discount);
                    custPrepStmt.setString(idx++, customer.c_credit);
                    custPrepStmt.setString(idx++, customer.c_last);
                    custPrepStmt.setString(idx++, customer.c_first);
                    custPrepStmt.setDouble(idx++, customer.c_credit_lim);
                    custPrepStmt.setDouble(idx++, customer.c_balance);
                    custPrepStmt.setDouble(idx++, customer.c_ytd_payment);
                    custPrepStmt.setLong(idx++, customer.c_payment_cnt);
                    custPrepStmt.setLong(idx++, customer.c_delivery_cnt);
                    custPrepStmt.setString(idx++, customer.c_street_1);
                    custPrepStmt.setString(idx++, customer.c_street_2);
                    custPrepStmt.setString(idx++, customer.c_city);
                    custPrepStmt.setString(idx++, customer.c_state);
                    custPrepStmt.setString(idx++, customer.c_zip);
                    custPrepStmt.setString(idx++, customer.c_phone);
                    custPrepStmt.setTimestamp(idx++, customer.c_since);
                    custPrepStmt.setString(idx++, customer.c_middle);
                    custPrepStmt.setString(idx++, customer.c_data);
                    custPrepStmt.setString(idx, customer.c_writeID);
                    custPrepStmt.execute();

                    var rs = custPrepStmt.getResultSet();
                    var p = customer.getInsertEventInfo(rs);
                    events.add(new InsertEvent(0, so, po, p, customer.getTableNames()));
                    ++po;
                }
            }

        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
        return po;
    }

    protected int loadCustomerHistory(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement histPrepStmt = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_HISTORY)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                for (int c = 1; c <= customersPerDistrict; c++) {
                    Timestamp sysdate = new Timestamp(System.currentTimeMillis());

                    var history = new HistoryHistory();
                    history.h_c_id = c;
                    history.h_c_d_id = d;
                    history.h_c_w_id = w_id;
                    history.h_d_id = d;
                    history.h_w_id = w_id;
                    history.h_date = sysdate;
                    history.h_amount = 10;
                    history.h_data = TPCCUtilHistory.randomStr(TPCCUtilHistory.randomNumber(10, 24, benchmark.rng()));
                    history.h_writeID = EventID.generateID(0, so, po);


                    int idx = 1;
                    histPrepStmt.setInt(idx++, history.h_c_id);
                    histPrepStmt.setInt(idx++, history.h_c_d_id);
                    histPrepStmt.setInt(idx++, history.h_c_w_id);
                    histPrepStmt.setInt(idx++, history.h_d_id);
                    histPrepStmt.setInt(idx++, history.h_w_id);
                    histPrepStmt.setTimestamp(idx++, history.h_date);
                    histPrepStmt.setDouble(idx++, history.h_amount);
                    histPrepStmt.setString(idx++, history.h_data);
                    histPrepStmt.setString(idx, history.h_writeID);
                    histPrepStmt.execute();

                    var rs = histPrepStmt.getResultSet();
                    var p = history.getInsertEventInfo(rs);
                    events.add(new InsertEvent(0, so, po, p, history.getTableNames()));
                    ++po;

                }
            }


        } catch (SQLException se) {
            LOG.error(se.getMessage());
        }
        return po;

    }

    protected int loadOpenOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement openOrderStatement = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_OPENORDER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {
                // TPC-C 4.3.3.1: o_c_id must be a permutation of [1, 3000]
                int[] c_ids = new int[customersPerDistrict];
                for (int i = 0; i < customersPerDistrict; ++i) {
                    c_ids[i] = i + 1;
                }
                // Collections.shuffle exists, but there is no
                // Arrays.shuffle
                for (int i = 0; i < c_ids.length - 1; ++i) {
                    int remaining = c_ids.length - i - 1;
                    int swapIndex = benchmark.rng().nextInt(remaining) + i + 1;

                    int temp = c_ids[swapIndex];
                    c_ids[swapIndex] = c_ids[i];
                    c_ids[i] = temp;
                }

                for (int c = 1; c <= customersPerDistrict; c++) {

                    OpenOrderHistory oorder = new OpenOrderHistory();
                    oorder.o_id = c;
                    oorder.o_w_id = w_id;
                    oorder.o_d_id = d;
                    oorder.o_c_id = c_ids[c - 1];
                    // o_carrier_id is set *only* for orders with ids < 2101
                    // [4.3.3.1]
                    if (oorder.o_id < FIRST_UNPROCESSED_O_ID) {
                        oorder.o_carrier_id = TPCCUtilHistory.randomNumber(1, 10, benchmark.rng());
                    } else {
                        oorder.o_carrier_id = null;
                    }
                    oorder.o_ol_cnt = getRandomCount(w_id, c, d);
                    oorder.o_all_local = 1;
                    oorder.o_entry_d = new Timestamp(System.currentTimeMillis());
                    oorder.o_writeID = EventID.generateID(0, so, po);

                    int idx = 1;
                    openOrderStatement.setInt(idx++, oorder.o_w_id);
                    openOrderStatement.setInt(idx++, oorder.o_d_id);
                    openOrderStatement.setInt(idx++, oorder.o_id);
                    openOrderStatement.setInt(idx++, oorder.o_c_id);
                    if (oorder.o_carrier_id != null) {
                        openOrderStatement.setInt(idx++, oorder.o_carrier_id);
                    } else {
                        openOrderStatement.setNull(idx++, Types.INTEGER);
                    }
                    openOrderStatement.setInt(idx++, oorder.o_ol_cnt);
                    openOrderStatement.setInt(idx++, oorder.o_all_local);
                    openOrderStatement.setTimestamp(idx++, oorder.o_entry_d);
                    openOrderStatement.setString(idx, oorder.o_writeID);
                    openOrderStatement.execute();

                    var rs = openOrderStatement.getResultSet();
                    var p = oorder.getInsertEventInfo(rs);
                    events.add(new InsertEvent(0, so, po, p, oorder.getTableNames()));
                    ++po;

                }

            }
        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }
        return po;

    }

    private int getRandomCount(int w_id, int c, int d) {
        CustomerHistory customer = new CustomerHistory();
        customer.c_id = c;
        customer.c_d_id = d;
        customer.c_w_id = w_id;

        Random random = new Random(customer.hashCode());

        return TPCCUtilHistory.randomNumber(5, 15, random);
    }

    protected int loadNewOrders(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement newOrderStatement = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_NEWORDER)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    // 900 rows in the NEW-ORDER table corresponding to the last
                    // 900 rows in the ORDER table for that district (i.e.,
                    // with NO_O_ID between 2,101 and 3,000)
                    if (c >= FIRST_UNPROCESSED_O_ID) {
                        NewOrderHistory new_order = new NewOrderHistory();
                        new_order.no_w_id = w_id;
                        new_order.no_d_id = d;
                        new_order.no_o_id = c;
                        new_order.no_writeID = EventID.generateID(0, so, po);

                        int idx = 1;
                        newOrderStatement.setInt(idx++, new_order.no_w_id);
                        newOrderStatement.setInt(idx++, new_order.no_d_id);
                        newOrderStatement.setInt(idx++, new_order.no_o_id);
                        newOrderStatement.setString(idx, new_order.no_writeID);

                        newOrderStatement.execute();
                        var rs = newOrderStatement.getResultSet();
                        var p = new_order.getInsertEventInfo(rs);
                        events.add(new InsertEvent(0, so, po, p, new_order.getTableNames()));
                        ++po;
                    }

                }

            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }
        return po;

    }

    protected int loadOrderLines(Connection conn, int w_id, int districtsPerWarehouse, int customersPerDistrict, ArrayList<Event> events, Integer so, Integer po) {


        try (PreparedStatement orderLineStatement = getInsertStatement(conn, TPCCConstantsHistory.TABLENAME_ORDERLINE)) {

            for (int d = 1; d <= districtsPerWarehouse; d++) {

                for (int c = 1; c <= customersPerDistrict; c++) {

                    int count = getRandomCount(w_id, c, d);

                    for (int l = 1; l <= count; l++) {
                        OrderLineHistory order_line = new OrderLineHistory();
                        order_line.ol_w_id = w_id;
                        order_line.ol_d_id = d;
                        order_line.ol_o_id = c;
                        order_line.ol_number = l; // ol_number
                        order_line.ol_i_id = TPCCUtilHistory.randomNumber(1, TPCCConfigHistory.configItemCount, benchmark.rng());
                        if (order_line.ol_o_id < FIRST_UNPROCESSED_O_ID) {
                            order_line.ol_delivery_d = new Timestamp(System.currentTimeMillis());
                            order_line.ol_amount = 0;
                        } else {
                            order_line.ol_delivery_d = null;
                            // random within [0.01 .. 9,999.99]
                            order_line.ol_amount = (float) (TPCCUtilHistory.randomNumber(1, 999999, benchmark.rng()) / 100.0);
                        }
                        order_line.ol_supply_w_id = order_line.ol_w_id;
                        order_line.ol_quantity = 5;
                        order_line.ol_dist_info = TPCCUtilHistory.randomStr(24);
                        order_line.ol_writeID = EventID.generateID(0, so, po);

                        int idx = 1;
                        orderLineStatement.setInt(idx++, order_line.ol_w_id);
                        orderLineStatement.setInt(idx++, order_line.ol_d_id);
                        orderLineStatement.setInt(idx++, order_line.ol_o_id);
                        orderLineStatement.setInt(idx++, order_line.ol_number);
                        orderLineStatement.setLong(idx++, order_line.ol_i_id);
                        if (order_line.ol_delivery_d != null) {
                            orderLineStatement.setTimestamp(idx++, order_line.ol_delivery_d);
                        } else {
                            orderLineStatement.setNull(idx++, 0);
                        }
                        orderLineStatement.setDouble(idx++, order_line.ol_amount);
                        orderLineStatement.setLong(idx++, order_line.ol_supply_w_id);
                        orderLineStatement.setDouble(idx++, order_line.ol_quantity);
                        orderLineStatement.setString(idx++, order_line.ol_dist_info);
                        orderLineStatement.setString(idx, order_line.ol_writeID);

                        orderLineStatement.execute();
                        var rs = orderLineStatement.getResultSet();
                        var p = order_line.getInsertEventInfo(rs);
                        events.add(new InsertEvent(0, so, po, p, order_line.getTableNames()));
                        ++po;
                    }

                }

            }

        } catch (SQLException se) {
            LOG.error(se.getMessage(), se);
        }
        return po;

    }

}

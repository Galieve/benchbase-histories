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

package com.oltpbenchmark.benchmarks.tpccNDHistories.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.apiHistory.events.*;

import com.oltpbenchmark.benchmarks.tpccHistories.TPCCConfigHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCUtilHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.*;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCConstantsHistory;
import com.oltpbenchmark.benchmarks.tpccNDHistories.TPCCWorkerNDHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Function;

public class NewOrderNDHistory extends TPCCProcedureNDHistory {

    private static final Logger LOG = LoggerFactory.getLogger(NewOrderNDHistory.class);

    public final SQLStmt stmtGetCustSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public final SQLStmt stmtGetWhseSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE W_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_WAREHOUSE));

    public final SQLStmt stmtGetDistSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE D_W_ID = ? AND D_ID = ? FOR UPDATE
    """.formatted(TPCCConstantsHistory.TABLENAME_DISTRICT));

    public final SQLStmt stmtInsertNewOrderSQL = new SQLStmt(
    """
        INSERT INTO %s
         (NO_O_ID, NO_D_ID, NO_W_ID, NO_DELIVERED, WRITEID)
         VALUES ( ?, ?, ?, ?, ?)
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_NEWORDER));

    public final SQLStmt stmtUpdateDistSQL = new SQLStmt(
    """
        UPDATE %s
           SET D_NEXT_O_ID = D_NEXT_O_ID + 1,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE D_W_ID = ?
           AND D_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_DISTRICT));

    public final SQLStmt stmtInsertOOrderSQL = new SQLStmt(
    """
        INSERT INTO %s
         (O_ID, O_D_ID, O_W_ID, O_C_ID, O_ENTRY_D, O_OL_CNT, O_ALL_LOCAL, WRITEID)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_OPENORDER));

    public final SQLStmt stmtGetItemSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE I_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_ITEM));

    public final SQLStmt stmtGetStockSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE S_I_ID = ?
           AND S_W_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_STOCK));

    public final SQLStmt stmtUpdateStockSQL = new SQLStmt(
    """
        UPDATE %s
           SET S_QUANTITY = ? ,
               S_YTD = S_YTD + ?,
               S_ORDER_CNT = S_ORDER_CNT + 1,
               S_REMOTE_CNT = S_REMOTE_CNT + ?,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE S_I_ID = ?
           AND S_W_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_STOCK));

    public final SQLStmt stmtInsertOrderLineSQL = new SQLStmt(
    """
        INSERT INTO %s
         (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER, OL_I_ID, OL_SUPPLY_W_ID, OL_QUANTITY, OL_AMOUNT, OL_DIST_INFO, WRITEID)
         VALUES (?,?,?,?,?,?,?,?,?,?)
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_ORDERLINE));

    public void run(Connection conn, Random gen, int terminalWarehouseID, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, long startTime, TPCCWorkerNDHistory w, ArrayList<Event> events, int id, int soID) throws SQLException {

        int districtID = TPCCUtilHistory.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int customerID = TPCCUtilHistory.getCustomerID(gen);

        int numItems = TPCCUtilHistory.randomNumber(5, 15, gen);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;

        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtilHistory.getItemID(gen);
            if (TPCCUtilHistory.randomNumber(1, 100, gen) > 1) {
                supplierWarehouseIDs[i] = terminalWarehouseID;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtilHistory.randomNumber(1, numWarehouses, gen);
                }
                while (supplierWarehouseIDs[i] == terminalWarehouseID && numWarehouses > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtilHistory.randomNumber(1, 10, gen);
        }

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtilHistory.randomNumber(1, 100, gen) == 1) {
            itemIDs[numItems - 1] = TPCCConfigHistory.INVALID_ITEM_ID;
        }

        newOrderTransaction(terminalWarehouseID, districtID, customerID, numItems, allLocal, itemIDs, supplierWarehouseIDs, orderQuantities, conn, events, id, soID);

    }


    private void newOrderTransaction(int w_id, int d_id, int c_id,
                                     int o_ol_cnt, int o_all_local, int[] itemIDs,
                                     int[] supplierWarehouseIDs, int[] orderQuantities, Connection conn, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;

        getCustomer(conn, w_id, d_id, c_id, id, soID, po, events);
        ++po;

        getWarehouse(conn, w_id, id, soID, po, events);
        ++po;

        int d_next_o_id = getDistrict(conn, w_id, d_id, id, soID, po, events);
        ++po;

        updateDistrict(conn, w_id, d_id, id, soID, po, events);
        ++po;

        insertOpenOrder(conn, w_id, d_id, c_id, o_ol_cnt, o_all_local, d_next_o_id, id, soID, po, events);
        ++po;

        insertNewOrder(conn, w_id, d_id, d_next_o_id, id, soID, po, events);
        ++po;

        try (PreparedStatement stmtUpdateStock = this.getPreparedStatement(conn, stmtUpdateStockSQL);
             PreparedStatement stmtInsertOrderLine = this.getPreparedStatement(conn, stmtInsertOrderLineSQL)) {

            for (int ol_number = 1; ol_number <= o_ol_cnt; ol_number++) {
                int ol_supply_w_id = supplierWarehouseIDs[ol_number - 1];
                int ol_i_id = itemIDs[ol_number - 1];
                int ol_quantity = orderQuantities[ol_number - 1];

                // this may occasionally error and that's ok!
                float i_price = getItemPrice(conn, ol_i_id, id, soID, po, events);
                ++po;

                float ol_amount = ol_quantity * i_price;

                StockHistory s = getStock(conn, ol_supply_w_id, ol_i_id, ol_quantity, id, soID, po, events);
                ++po;

                //This is not a SQL query
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
                stmtInsertOrderLine.setString(10, EventID.generateID(id, soID, po));

                stmtInsertOrderLine.execute();
                var rs = stmtInsertOrderLine.getResultSet();
                var ol = new OrderLineHistory();
                var p = ol.getInsertEventInfo(rs);
                events.add(new InsertEvent(id, soID, po, p, ol.getTableNames()));
                ++po;

                int s_remote_cnt_increment;

                if (ol_supply_w_id == w_id) {
                    s_remote_cnt_increment = 0;
                } else {
                    s_remote_cnt_increment = 1;
                }

                var evID = EventID.generateID(id, soID, po);
                stmtUpdateStock.setInt(1, s.s_quantity);
                stmtUpdateStock.setInt(2, ol_quantity);
                stmtUpdateStock.setInt(3, s_remote_cnt_increment);
                stmtUpdateStock.setString(4, evID);
                stmtUpdateStock.setInt(5, ol_i_id);
                stmtUpdateStock.setInt(6, ol_supply_w_id);
                stmtUpdateStock.execute();


                rs = stmtUpdateStock.getResultSet();
                Function<Value, Value> set = (val)->{
                    val.setValue("S_QUANTITY", String.valueOf(s.s_quantity));
                    val.setValue("S_YTD", String.valueOf(Float.parseFloat(val.getValue("S_YTD")) + ol_quantity));
                    val.setValue("S_ORDER_CNT", String.valueOf(Integer.parseInt(val.getValue("S_ORDER_CNT")) + 1));
                    val.setValue("S_REMOTE_CNT", String.valueOf(Integer.parseInt(val.getValue("S_REMOTE_CNT")) + s_remote_cnt_increment));
                    val.setValue("WRITEID", evID);
                    return val;
                };
                var infoUpdate = s.getUpdateEventInfo(set, rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    String.valueOf(ol_supply_w_id).equals(val.getValue("S_W_ID")) &&
                    String.valueOf(ol_i_id).equals(val.getValue("S_I_ID"));
                events.add(new UpdateEvent(id, soID, po, infoUpdate.first, infoUpdate.second, where, s.getTableNames()));
                ++po;
            }
        }

    }

    private String getDistInfo(int d_id, StockHistory s) {
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

    private StockHistory getStock(Connection conn, int ol_supply_w_id, int ol_i_id, int ol_quantity, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stmtGetStock = this.getPreparedStatement(conn, stmtGetStockSQL)) {
            stmtGetStock.setInt(1, ol_i_id);
            stmtGetStock.setInt(2, ol_supply_w_id);
            try (ResultSet rs = stmtGetStock.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("S_I_ID=" + ol_i_id + " not found!");
                }
                StockHistory s = new StockHistory();
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

                var p = s.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    String.valueOf(ol_i_id).equals(val.getValue("S_I_ID")) &&
                    String.valueOf(ol_supply_w_id).equals(val.getValue("S_W_ID"));
                events.add(new SelectEvent(id, so, po, p, where, s.getTableNames()));

                return s;
            }
        }
    }

    private float getItemPrice(Connection conn, int ol_i_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stmtGetItem = this.getPreparedStatement(conn, stmtGetItemSQL)) {
            stmtGetItem.setInt(1, ol_i_id);
            try (ResultSet rs = stmtGetItem.executeQuery()) {

                Float ret = null;
                if(rs.next()){
                    ret = rs.getFloat("I_PRICE");
                }
                var i = new ItemHistory();
                var p = i.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(ol_i_id).equals(val.getValue("I_ID"));
                events.add(new SelectEvent(id, so, po, p, where, i.getTableNames()));
                if (ret == null) {
                    // This is (hopefully) an expected error: this is an expected new order rollback
                    throw new UserAbortException("EXPECTED new order rollback: I_ID=" + ol_i_id + " not found!");

                }
                return ret;
            }
        }
    }

    private void insertNewOrder(Connection conn, int w_id, int d_id, int o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);

        try (PreparedStatement stmtInsertNewOrder = this.getPreparedStatement(conn, stmtInsertNewOrderSQL);) {
            stmtInsertNewOrder.setInt(1, o_id);
            stmtInsertNewOrder.setInt(2, d_id);
            stmtInsertNewOrder.setInt(3, w_id);
            stmtInsertNewOrder.setBoolean(4, false);
            stmtInsertNewOrder.setString(5, evID);
            stmtInsertNewOrder.execute();
            int result = stmtInsertNewOrder.getUpdateCount();

            if (result == 0) {
                LOG.warn("new order not inserted");
            }
            var rs = stmtInsertNewOrder.getResultSet();
            var no = new com.oltpbenchmark.benchmarks.tpccHistories.pojo.NewOrderHistory();
            var p = no.getInsertEventInfo(rs);
            events.add(new InsertEvent(id, so, po, p, no.getTableNames()));
        }
    }

    private void insertOpenOrder(Connection conn, int w_id, int d_id, int c_id, int o_ol_cnt, int o_all_local, int o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);

        try (PreparedStatement stmtInsertOOrder = this.getPreparedStatement(conn, stmtInsertOOrderSQL);) {
            stmtInsertOOrder.setInt(1, o_id);
            stmtInsertOOrder.setInt(2, d_id);
            stmtInsertOOrder.setInt(3, w_id);
            stmtInsertOOrder.setInt(4, c_id);
            stmtInsertOOrder.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
            stmtInsertOOrder.setInt(6, o_ol_cnt);
            stmtInsertOOrder.setInt(7, o_all_local);
            stmtInsertOOrder.setString(8, evID);

            stmtInsertOOrder.execute();
            int result = stmtInsertOOrder.getUpdateCount();

            if (result == 0) {
                LOG.warn("open order not inserted");
            }
            var rs = stmtInsertOOrder.getResultSet();
            var oo = new OpenOrderHistory();
            var p = oo.getInsertEventInfo(rs);
            events.add(new InsertEvent(id, so, po, p, oo.getTableNames()));
        }
    }

    private void updateDistrict(Connection conn, int w_id, int d_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);
        try (PreparedStatement stmtUpdateDist = this.getPreparedStatement(conn, stmtUpdateDistSQL)) {
            stmtUpdateDist.setString(1, evID);
            stmtUpdateDist.setInt(2, w_id);
            stmtUpdateDist.setInt(3, d_id);

            stmtUpdateDist.execute();
            int result = stmtUpdateDist.getUpdateCount();

            if (result == 0) {
                throw new RuntimeException("Error!! Cannot update next_order_id on district for D_ID=" + d_id + " D_W_ID=" + w_id);
            }
            var rs = stmtUpdateDist.getResultSet();
            Function<Value, Value> set = (val)->{
                val.setValue("D_NEXT_O_ID",
                    String.valueOf(Integer.parseInt(val.getValue("D_NEXT_O_ID")) + 1));
                val.setValue("WRITEID", evID);
                return val;
            };
            var d = new DistrictHistory();
            var p = d.getUpdateEventInfo(set, rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("D_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("D_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, d.getTableNames()));
        }
    }

    private int getDistrict(Connection conn, int w_id, int d_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stmtGetDist = this.getPreparedStatement(conn, stmtGetDistSQL)) {
            stmtGetDist.setInt(1, w_id);
            stmtGetDist.setInt(2, d_id);
            try (ResultSet rs = stmtGetDist.executeQuery()) {

                if (!rs.next()) {
                    throw new RuntimeException("D_ID=" + d_id + " D_W_ID=" + w_id + " not found!");
                }
                var ret = rs.getInt("D_NEXT_O_ID");
                var d = new DistrictHistory();
                var p = d.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("D_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("D_ID"));
                events.add(new SelectEvent(id, so, po, p, where, d.getTableNames()));
                return ret;
            }
        }
    }

    private void getWarehouse(Connection conn, int w_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stmtGetWhse = this.getPreparedStatement(conn, stmtGetWhseSQL)) {
            stmtGetWhse.setInt(1, w_id);
            try (ResultSet rs = stmtGetWhse.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }
                var w = new WarehouseHistory();
                var p = w.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("W_ID"));
                events.add(new SelectEvent(id, so, po, p, where, w.getTableNames()));
            }
        }
    }

    private void getCustomer(Connection conn, int w_id, int d_id, int c_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stmtGetCust = this.getPreparedStatement(conn, stmtGetCustSQL)) {
            stmtGetCust.setInt(1, w_id);
            stmtGetCust.setInt(2, d_id);
            stmtGetCust.setInt(3, c_id);
            try (ResultSet rs = stmtGetCust.executeQuery()) {
                if (!rs.next()) {
                    throw new RuntimeException("C_D_ID=" + d_id + " C_ID=" + c_id + " not found!");
                }
                var c = new CustomerHistory();
                var p = c.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("C_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("C_D_ID")) &&
                    String.valueOf(c_id).equals(val.getValue("C_ID"));
                events.add(new SelectEvent(id, so, po, p, where, c.getTableNames()));
            }
        }
    }

}

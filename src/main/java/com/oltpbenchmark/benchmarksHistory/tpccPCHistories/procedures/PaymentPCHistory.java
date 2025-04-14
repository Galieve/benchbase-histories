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

package com.oltpbenchmark.benchmarksHistory.tpccPCHistories.procedures;

import com.oltpbenchmark.historyModelHistory.events.*;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.TPCCConfigHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.TPCCConstantsHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.TPCCUtilHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.CustomerHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.DistrictHistory;
import com.oltpbenchmark.benchmarksHistory.tpccPCHistories.TPCCPCWorkerHistory;
import com.oltpbenchmark.historyModelHistory.events.*;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.HistoryHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.WarehouseHistory;
import com.oltpbenchmark.utilHistory.SQLUtilHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Function;

public class PaymentPCHistory extends TPCCProcedureNDHistory {

    private static final Logger LOG = LoggerFactory.getLogger(PaymentPCHistory.class);

    public SQLStmt payUpdateWhseSQL = new SQLStmt(
    """
        UPDATE %s
           SET W_YTD = W_YTD + ?,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE W_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_WAREHOUSE));

    public SQLStmt payGetWhseSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE W_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_WAREHOUSE));

    public SQLStmt payUpdateDistSQL = new SQLStmt(
    """
        UPDATE %s
           SET D_YTD = D_YTD + ?,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE D_W_ID = ?
           AND D_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_DISTRICT));

    public SQLStmt payGetDistSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE D_W_ID = ?
           AND D_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_DISTRICT));

    public SQLStmt payGetCustSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public SQLStmt payGetCustCdataSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public SQLStmt payUpdateCustBalCdataSQL = new SQLStmt(
    """
        UPDATE %s
           SET C_BALANCE = ?,
               C_YTD_PAYMENT = ?,
               C_PAYMENT_CNT = ?,
               C_DATA = ?,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public SQLStmt payUpdateCustBalSQL = new SQLStmt(
    """
        UPDATE %s
           SET C_BALANCE = ?,
               C_YTD_PAYMENT = ?,
               C_PAYMENT_CNT = ?,
               WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public SQLStmt payInsertHistSQL = new SQLStmt(
    """
        INSERT INTO %s
         (H_C_D_ID, H_C_W_ID, H_C_ID, H_D_ID, H_W_ID, H_DATE, H_AMOUNT, H_DATA, WRITEID)
         VALUES (?,?,?,?,?,?,?,?, ?)
         RETURNING *
    """.formatted(TPCCConstantsHistory.TABLENAME_HISTORY));

    public SQLStmt customerByNameSQL = new SQLStmt(
    """
        SELECT *
          FROM %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_LAST = ?
         ORDER BY C_FIRST
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, long startTime, TPCCPCWorkerHistory worker, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;

        int districtID = TPCCUtilHistory.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        float paymentAmount = (float) (TPCCUtilHistory.randomNumber(100, 500000, gen) / 100.0);

        updateWarehouse(conn, w_id, paymentAmount, id, soID, po, events);
        ++po;

        WarehouseHistory w = getWarehouse(conn, w_id, id, soID, po, events);
        ++po;

        updateDistrict(conn, w_id, districtID, paymentAmount, id, soID, po, events);
        ++po;

        DistrictHistory d = getDistrict(conn, w_id, districtID, id, soID, po, events);
        ++po;

        int x = TPCCUtilHistory.randomNumber(1, 100, gen);

        //NO SQL query
        int customerDistrictID = getCustomerDistrictId(gen, districtID, x);

        //NO SQL query
        int customerWarehouseID = getCustomerWarehouseID(gen, w_id, numWarehouses, x);

        CustomerHistory c = getCustomer(conn, gen, customerDistrictID, customerWarehouseID, paymentAmount, id, soID, po, events);
        ++po;

        if (c.c_credit.equals("BC")) {
            // bad credit
            c.c_data = getCData(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, c, id, soID, po, events);
            ++po;

            updateBalanceCData(conn, customerDistrictID, customerWarehouseID, c, id, soID, po, events);
            ++po;

        } else {
            // GoodCredit

            updateBalance(conn, customerDistrictID, customerWarehouseID, c, id, soID, po, events);
            ++po;

        }

        insertHistory(conn, w_id, districtID, customerDistrictID, customerWarehouseID, paymentAmount, w.w_name, d.d_name, c, id, soID, po, events);
        ++po;

        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+---------------------------- PAYMENT ----------------------------+");
            terminalMessage.append("\n Date: ").append(TPCCUtilHistory.getCurrentTime());
            terminalMessage.append("\n\n WarehouseHistory: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(w.w_street_1);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(w.w_street_2);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(w.w_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(w.w_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(w.w_zip);
            terminalMessage.append("\n\n DistrictHistory:  ");
            terminalMessage.append(districtID);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(d.d_street_1);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(d.d_street_2);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(d.d_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(d.d_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(d.d_zip);
            terminalMessage.append("\n\n User:  ");
            terminalMessage.append(c.c_id);
            terminalMessage.append("\n   Name:    ");
            terminalMessage.append(c.c_first);
            terminalMessage.append(" ");
            terminalMessage.append(c.c_middle);
            terminalMessage.append(" ");
            terminalMessage.append(c.c_last);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(c.c_street_1);
            terminalMessage.append("\n   Street:  ");
            terminalMessage.append(c.c_street_2);
            terminalMessage.append("\n   City:    ");
            terminalMessage.append(c.c_city);
            terminalMessage.append("   State: ");
            terminalMessage.append(c.c_state);
            terminalMessage.append("  Zip: ");
            terminalMessage.append(c.c_zip);
            terminalMessage.append("\n   Since:   ");
            if (c.c_since != null) {
                terminalMessage.append(c.c_since.toString());
            } else {
                terminalMessage.append("");
            }
            terminalMessage.append("\n   Credit:  ");
            terminalMessage.append(c.c_credit);
            terminalMessage.append("\n   %Disc:   ");
            terminalMessage.append(c.c_discount);
            terminalMessage.append("\n   Phone:   ");
            terminalMessage.append(c.c_phone);
            terminalMessage.append("\n\n Amount Paid:      ");
            terminalMessage.append(paymentAmount);
            terminalMessage.append("\n Credit Limit:     ");
            terminalMessage.append(c.c_credit_lim);
            terminalMessage.append("\n New Cust-Balance: ");
            terminalMessage.append(c.c_balance);
            if (c.c_credit.equals("BC")) {
                if (c.c_data.length() > 50) {
                    terminalMessage.append("\n\n Cust-Data: ").append(c.c_data.substring(0, 50));
                    int data_chunks = c.c_data.length() > 200 ? 4 : c.c_data.length() / 50;
                    for (int n = 1; n < data_chunks; n++) {
                        terminalMessage.append("\n            ").append(c.c_data.substring(n * 50, (n + 1) * 50));
                    }
                } else {
                    terminalMessage.append("\n\n Cust-Data: ").append(c.c_data);
                }
            }
            terminalMessage.append("\n+-----------------------------------------------------------------+\n\n");

            LOG.trace(terminalMessage.toString());

        }

    }

    private int getCustomerWarehouseID(Random gen, int w_id, int numWarehouses, int x) {
        int customerWarehouseID;
        if (x <= 85) {
            customerWarehouseID = w_id;
        } else {
            do {
                customerWarehouseID = TPCCUtilHistory.randomNumber(1, numWarehouses, gen);
            }
            while (customerWarehouseID == w_id && numWarehouses > 1);
        }
        return customerWarehouseID;
    }

    private int getCustomerDistrictId(Random gen, int districtID, int x) {
        if (x <= 85) {
            return districtID;
        } else {
            return TPCCUtilHistory.randomNumber(1, TPCCConfigHistory.configDistPerWhse, gen);
        }


    }

    private void updateWarehouse(Connection conn, int w_id, float paymentAmount, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);

        try (PreparedStatement payUpdateWhse = this.getPreparedStatement(conn, payUpdateWhseSQL)) {
            payUpdateWhse.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
            payUpdateWhse.setString(2, evID);
            payUpdateWhse.setInt(3, w_id);
            // MySQL reports deadlocks due to lock upgrades:
            // t1: read w_id = x; t2: update w_id = x; t1 update w_id = x
            payUpdateWhse.execute();

            var rs = payUpdateWhse.getResultSet();

            int result = SQLUtilHistory.size(rs);


            var w = new WarehouseHistory();
            var p = w.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("W_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, w.getTableNames()));

            if (result == 0) {
                throw new RuntimeException("W_ID=" + w_id + " not found!");
            }
        }
    }

    private WarehouseHistory getWarehouse(Connection conn, int w_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement payGetWhse = this.getPreparedStatement(conn, payGetWhseSQL)) {
            payGetWhse.setInt(1, w_id);

            try (ResultSet rs = payGetWhse.executeQuery()) {

                WarehouseHistory w = new WarehouseHistory();

                var p = w.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("W_ID"));
                events.add(new SelectEvent(id, so, po, p, where, w.getTableNames()));

                rs.beforeFirst();
                if (!rs.next()) {
                    throw new RuntimeException("W_ID=" + w_id + " not found!");
                }

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

    private CustomerHistory getCustomer(Connection conn, Random gen, int customerDistrictID, int customerWarehouseID, float paymentAmount, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        int y = TPCCUtilHistory.randomNumber(1, 100, gen);

        CustomerHistory c;

        if (y <= 60) {
            // 60% lookups by last name
            c = getCustomerByName(customerWarehouseID, customerDistrictID, TPCCUtilHistory.getNonUniformRandomLastNameForRun(gen), conn, id, so, po, events);
        } else {
            // 40% lookups by customer ID
            c = getCustomerById(customerWarehouseID, customerDistrictID, TPCCUtilHistory.getCustomerID(gen), conn, id, so, po, events);
        }

        c.c_balance -= paymentAmount;
        c.c_ytd_payment += paymentAmount;
        c.c_payment_cnt += 1;

        return c;
    }

    private void updateDistrict(Connection conn, int w_id, int districtID, float paymentAmount, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);


        try (PreparedStatement payUpdateDist = this.getPreparedStatement(conn, payUpdateDistSQL)) {
            payUpdateDist.setBigDecimal(1, BigDecimal.valueOf(paymentAmount));
            payUpdateDist.setString(2, evID);
            payUpdateDist.setInt(3, w_id);
            payUpdateDist.setInt(4, districtID);

            payUpdateDist.execute();

            var rs = payUpdateDist.getResultSet();
            int result = SQLUtilHistory.size(rs);

            var d = new DistrictHistory();
            var p = d.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("D_W_ID")) &&
                String.valueOf(districtID).equals(val.getValue("D_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, d.getTableNames()));

            if (result == 0) {
                throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
            }
        }
    }

    private DistrictHistory getDistrict(Connection conn, int w_id, int districtID, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement payGetDist = this.getPreparedStatement(conn, payGetDistSQL)) {
            payGetDist.setInt(1, w_id);
            payGetDist.setInt(2, districtID);

            try (ResultSet rs = payGetDist.executeQuery()) {

                DistrictHistory d = new DistrictHistory();

                var p = d.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("D_W_ID")) &&
                    String.valueOf(districtID).equals(val.getValue("D_ID"));
                events.add(new SelectEvent(id, so, po, p, where, d.getTableNames()));

                rs.beforeFirst();
                if (!rs.next()) {
                    throw new RuntimeException("D_ID=" + districtID + " D_W_ID=" + w_id + " not found!");
                }

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

    private String getCData(Connection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        try (PreparedStatement payGetCustCdata = this.getPreparedStatement(conn, payGetCustCdataSQL)) {
            String c_data;
            payGetCustCdata.setInt(1, customerWarehouseID);
            payGetCustCdata.setInt(2, customerDistrictID);
            payGetCustCdata.setInt(3, c.c_id);
            try (ResultSet rs = payGetCustCdata.executeQuery()) {

                var p = c.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(customerWarehouseID).equals(val.getValue("C_W_ID")) &&
                    String.valueOf(customerDistrictID).equals(val.getValue("C_D_ID")) &&
                    String.valueOf(c.c_id).equals(val.getValue("C_ID"));
                events.add(new SelectEvent(id, so, po, p, where, c.getTableNames()));

                rs.beforeFirst();
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

    private void updateBalanceCData(Connection conn, int customerDistrictID, int customerWarehouseID, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);

        try (PreparedStatement payUpdateCustBalCdata = this.getPreparedStatement(conn, payUpdateCustBalCdataSQL)) {
            payUpdateCustBalCdata.setDouble(1, c.c_balance);
            payUpdateCustBalCdata.setDouble(2, c.c_ytd_payment);
            payUpdateCustBalCdata.setInt(3, c.c_payment_cnt);
            payUpdateCustBalCdata.setString(4, c.c_data);
            payUpdateCustBalCdata.setString(5, evID);
            payUpdateCustBalCdata.setInt(6, customerWarehouseID);
            payUpdateCustBalCdata.setInt(7, customerDistrictID);
            payUpdateCustBalCdata.setInt(8, c.c_id);

            payUpdateCustBalCdata.execute();
            var rs = payUpdateCustBalCdata.getResultSet();
            int result = SQLUtilHistory.size(rs);

            var p = c.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(customerWarehouseID).equals(val.getValue("C_W_ID")) &&
                String.valueOf(customerDistrictID).equals(val.getValue("C_D_ID")) &&
                String.valueOf(c.c_id).equals(val.getValue("C_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, c.getTableNames()));

            if (result == 0) {
                throw new RuntimeException("Error in PYMNT Txn updating User C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID);
            }
        }
    }

    private void updateBalance(Connection conn, int customerDistrictID, int customerWarehouseID, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);


        try (PreparedStatement payUpdateCustBal = this.getPreparedStatement(conn, payUpdateCustBalSQL)) {
            payUpdateCustBal.setDouble(1, c.c_balance);
            payUpdateCustBal.setDouble(2, c.c_ytd_payment);
            payUpdateCustBal.setInt(3, c.c_payment_cnt);
            payUpdateCustBal.setString(4, evID);
            payUpdateCustBal.setInt(5, customerWarehouseID);
            payUpdateCustBal.setInt(6, customerDistrictID);
            payUpdateCustBal.setInt(7, c.c_id);

            payUpdateCustBal.execute();

            var rs = payUpdateCustBal.getResultSet();

            int result = SQLUtilHistory.size(rs);

            var p = c.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(customerWarehouseID).equals(val.getValue("C_W_ID")) &&
                String.valueOf(customerDistrictID).equals(val.getValue("C_D_ID")) &&
                String.valueOf(c.c_id).equals(val.getValue("C_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, c.getTableNames()));

            if (result == 0) {
                throw new RuntimeException("C_ID=" + c.c_id + " C_W_ID=" + customerWarehouseID + " C_D_ID=" + customerDistrictID + " not found!");
            }
        }
    }

    private void insertHistory(Connection conn, int w_id, int districtID, int customerDistrictID, int customerWarehouseID, float paymentAmount, String w_name, String d_name, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        if (w_name.length() > 10) {
            w_name = w_name.substring(0, 10);
        }
        if (d_name.length() > 10) {
            d_name = d_name.substring(0, 10);
        }
        String h_data = w_name + "    " + d_name;
        var evID = EventID.generateID(id, so, po);


        try (PreparedStatement payInsertHist = this.getPreparedStatement(conn, payInsertHistSQL)) {
            payInsertHist.setInt(1, customerDistrictID);
            payInsertHist.setInt(2, customerWarehouseID);
            payInsertHist.setInt(3, c.c_id);
            payInsertHist.setInt(4, districtID);
            payInsertHist.setInt(5, w_id);
            payInsertHist.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            payInsertHist.setDouble(7, paymentAmount);
            payInsertHist.setString(8, h_data);
            payInsertHist.setString(9, evID);
            payInsertHist.execute();

            var rs = payInsertHist.getResultSet();

            var h = new HistoryHistory();
            var p = h.getInsertEventInfo(rs);
            events.add(new InsertEvent(id, so, po, p, h.getTableNames()));

        }
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public CustomerHistory getCustomerById(int c_w_id, int c_d_id, int c_id, Connection conn, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        try (PreparedStatement payGetCust = this.getPreparedStatement(conn, payGetCustSQL)) {

            payGetCust.setInt(1, c_w_id);
            payGetCust.setInt(2, c_d_id);
            payGetCust.setInt(3, c_id);

            try (ResultSet rs = payGetCust.executeQuery()) {

                var c = new CustomerHistory();
                var p = c.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(c_w_id).equals(val.getValue("C_W_ID")) &&
                    String.valueOf(c_d_id).equals(val.getValue("C_D_ID")) &&
                    String.valueOf(c_id).equals(val.getValue("C_ID"));
                events.add(new SelectEvent(id, so, po, p, where, c.getTableNames()));

                if (!rs.next()) {
                    throw new RuntimeException("C_ID=" + c_id + " C_D_ID=" + c_d_id + " C_W_ID=" + c_w_id + " not found!");
                }

                c = TPCCUtilHistory.newCustomerFromResults(rs);
                c.c_id = c_id;
                c.c_last = rs.getString("C_LAST");
                return c;
            }
        }
    }

    // attention this code is repeated in other transacitons... ok for now to
    // allow for separate statements.
    public CustomerHistory getCustomerByName(int c_w_id, int c_d_id, String customerLastName, Connection conn, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        ArrayList<CustomerHistory> customers = new ArrayList<>();

        try (PreparedStatement customerByName = this.getPreparedStatement(conn, customerByNameSQL)) {

            customerByName.setInt(1, c_w_id);
            customerByName.setInt(2, c_d_id);
            customerByName.setString(3, customerLastName);
            try (ResultSet rs = customerByName.executeQuery()) {

                if (LOG.isTraceEnabled()) {
                    LOG.trace("C_LAST={} C_D_ID={} C_W_ID={}", customerLastName, c_d_id, c_w_id);
                }


                var ch = new CustomerHistory();
                var p = ch.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(c_w_id).equals(val.getValue("C_W_ID")) &&
                    String.valueOf(c_d_id).equals(val.getValue("C_D_ID")) &&
                    String.valueOf(customerLastName).equals(val.getValue("C_LAST"));
                events.add(new SelectEvent(id, so, po, p, where, ch.getTableNames()));

                rs.beforeFirst();

                while (rs.next()) {
                    CustomerHistory c = TPCCUtilHistory.newCustomerFromResults(rs);
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

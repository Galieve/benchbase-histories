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
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.SelectEvent;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCConstantsHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCUtilHistory;
import com.oltpbenchmark.benchmarks.tpccNDHistories.TPCCWorkerNDHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.CustomerHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.OpenOrderHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.OrderLineHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class OrderStatusNDHistory extends TPCCProcedureNDHistory {

    private static final Logger LOG = LoggerFactory.getLogger(OrderStatusNDHistory.class);

    public SQLStmt ordStatGetNewestOrdSQL = new SQLStmt(
    """
        SELECT *
          FROM  %s
         WHERE O_W_ID = ?
           AND O_D_ID = ?
           AND O_C_ID = ?
         ORDER BY O_ID DESC
    """.formatted(TPCCConstantsHistory.TABLENAME_OPENORDER));

    public SQLStmt ordStatGetOrderLinesSQL = new SQLStmt(
    """
        SELECT *
          FROM  %s
         WHERE OL_O_ID = ?
           AND OL_D_ID = ?
           AND OL_W_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_ORDERLINE));

    public SQLStmt payGetCustSQL = new SQLStmt(
    """
        SELECT *
          FROM  %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public SQLStmt customerByNameSQL = new SQLStmt(
    """
        SELECT *
          FROM  %s
         WHERE C_W_ID = ?
           AND C_D_ID = ?
           AND C_LAST = ?
         ORDER BY C_FIRST
    """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, long startTime, TPCCWorkerNDHistory w, ArrayList<Event> events, int id, int soID) throws SQLException {

        int d_id = TPCCUtilHistory.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);
        int y = TPCCUtilHistory.randomNumber(1, 100, gen);

        boolean c_by_name;
        String c_last = null;
        int c_id = -1;

        if (y <= 60) {
            c_by_name = true;
            c_last = TPCCUtilHistory.getNonUniformRandomLastNameForRun(gen);
        } else {
            c_by_name = false;
            c_id = TPCCUtilHistory.getCustomerID(gen);
        }


        CustomerHistory c;
        int po = 0;

        if (c_by_name) {
            c = getCustomerByName(w_id, d_id, c_last, id, soID, po, events, conn);
            ++po;
        } else {
            c = getCustomerById(w_id, d_id, c_id, id, soID, po, events, conn);
            ++po;
        }


        OpenOrderHistory o = getOrderDetails(conn, w_id, d_id, c, id, soID, po, events);
        ++po;

        // retrieve the order lines for the most recent order
        List<String> orderLines = getOrderLines(conn, w_id, d_id, o.o_id, c, id, soID, po, events);
        ++po;

        if (LOG.isTraceEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\n");
            sb.append("+-------------------------- ORDER-STATUS -------------------------+\n");
            sb.append(" Date: ");
            sb.append(TPCCUtilHistory.getCurrentTime());
            sb.append("\n\n WarehouseHistory: ");
            sb.append(w_id);
            sb.append("\n DistrictHistory:  ");
            sb.append(d_id);
            sb.append("\n\n User:  ");
            sb.append(c.c_id);
            sb.append("\n   Name:    ");
            sb.append(c.c_first);
            sb.append(" ");
            sb.append(c.c_middle);
            sb.append(" ");
            sb.append(c.c_last);
            sb.append("\n   Balance: ");
            sb.append(c.c_balance);
            sb.append("\n\n");
            if (o.o_id == -1) {
                sb.append(" User has no orders placed.\n");
            } else {
                sb.append(" Order-Number: ");
                sb.append(o.o_id);
                sb.append("\n    Entry-Date: ");
                sb.append(o.o_entry_d);
                sb.append("\n    Carrier-Number: ");
                sb.append(o.o_carrier_id);
                sb.append("\n\n");
                if (orderLines.size() != 0) {
                    sb.append(" [Supply_W - Item_ID - Qty - Amount - DeliveryNDHistory-Date]\n");
                    for (String orderLine : orderLines) {
                        sb.append(" ");
                        sb.append(orderLine);
                        sb.append("\n");
                    }
                } else {
                    LOG.trace(" This Order has no Order-Lines.\n");
                }
            }
            sb.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(sb.toString());
        }


    }

    private OpenOrderHistory getOrderDetails(Connection conn, int w_id, int d_id, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement ordStatGetNewestOrd = this.getPreparedStatement(conn, ordStatGetNewestOrdSQL)) {


            // find the newest order for the customer
            // retrieve the carrier & order date for the most recent order.

            ordStatGetNewestOrd.setInt(1, w_id);
            ordStatGetNewestOrd.setInt(2, d_id);
            ordStatGetNewestOrd.setInt(3, c.c_id);

            try (ResultSet rs = ordStatGetNewestOrd.executeQuery()) {

                OpenOrderHistory o = new OpenOrderHistory();

                var p = o.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("O_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("O_D_ID")) &&
                    String.valueOf(c.c_id).equals(val.getValue("O_C_ID"));
                events.add(new SelectEvent(id, so, po, p, where, o.getTableNames()));

                rs.beforeFirst();
                if (!rs.next()) {
                    String msg = String.format("No order records for CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]", w_id, d_id, c.c_id);

                    throw new RuntimeException(msg);
                }
                o.o_id=rs.getInt("O_ID");
                o.o_carrier_id = rs.getInt("O_CARRIER_ID");
                o.o_entry_d = rs.getTimestamp("O_ENTRY_D");
                return o;
            }
        }
    }

    private List<String> getOrderLines(Connection conn, int w_id, int d_id, int o_id, CustomerHistory c, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        List<String> orderLines = new ArrayList<>();

        try (PreparedStatement ordStatGetOrderLines = this.getPreparedStatement(conn, ordStatGetOrderLinesSQL)) {
            ordStatGetOrderLines.setInt(1, o_id);
            ordStatGetOrderLines.setInt(2, d_id);
            ordStatGetOrderLines.setInt(3, w_id);

            try (ResultSet rs = ordStatGetOrderLines.executeQuery()) {
                var ol = new OrderLineHistory();
                var p = ol.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("OL_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("OL_D_ID")) &&
                    String.valueOf(o_id).equals(val.getValue("OL_O_ID"));
                events.add(new SelectEvent(id, so, po, p, where, ol.getTableNames()));

                rs.beforeFirst();
                while (rs.next()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("[");
                    sb.append(rs.getLong("OL_SUPPLY_W_ID"));
                    sb.append(" - ");
                    sb.append(rs.getLong("OL_I_ID"));
                    sb.append(" - ");
                    sb.append(rs.getLong("OL_QUANTITY"));
                    sb.append(" - ");
                    sb.append(TPCCUtilHistory.formattedDouble(rs.getDouble("OL_AMOUNT")));
                    sb.append(" - ");
                    if (rs.getTimestamp("OL_DELIVERY_D") != null) {
                        sb.append(rs.getTimestamp("OL_DELIVERY_D"));
                    } else {
                        sb.append("99-99-9999");
                    }
                    sb.append("]");
                    orderLines.add(sb.toString());
                }
            }


            if (orderLines.isEmpty()) {
                String msg = String.format("Order record had no order line items [C_W_ID=%d, C_D_ID=%d, C_ID=%d, O_ID=%d]", w_id, d_id, c.c_id, o_id);
                LOG.trace(msg);
            }
        }

        return orderLines;
    }

    // attention duplicated code across trans... ok for now to maintain separate
    // prepared statements
    public CustomerHistory getCustomerById(int c_w_id, int c_d_id, int c_id, Integer id, Integer so, Integer po, ArrayList<Event> events, Connection conn) throws SQLException {

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

                rs.beforeFirst();
                if (!rs.next()) {
                    String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_ID=%d]", c_w_id, c_d_id, c_id);

                    throw new RuntimeException(msg);
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
    public CustomerHistory getCustomerByName(int c_w_id, int c_d_id, String c_last, Integer id, Integer so, Integer po, ArrayList<Event> events, Connection conn) throws SQLException {
        ArrayList<CustomerHistory> customers = new ArrayList<>();

        try (PreparedStatement customerByName = this.getPreparedStatement(conn, customerByNameSQL)) {

            customerByName.setInt(1, c_w_id);
            customerByName.setInt(2, c_d_id);
            customerByName.setString(3, c_last);

            try (ResultSet rs = customerByName.executeQuery()) {

                var ch = new CustomerHistory();
                var p = ch.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(c_w_id).equals(val.getValue("C_W_ID")) &&
                    String.valueOf(c_d_id).equals(val.getValue("C_D_ID")) &&
                    String.valueOf(c_last).equals(val.getValue("C_LAST"));
                events.add(new SelectEvent(id, so, po, p, where, ch.getTableNames()));

                rs.beforeFirst();
                while (rs.next()) {
                    CustomerHistory c = TPCCUtilHistory.newCustomerFromResults(rs);
                    c.c_id = rs.getInt("C_ID");
                    c.c_last = c_last;
                    customers.add(c);
                }
            }
        }

        if (customers.size() == 0) {
            String msg = String.format("Failed to get CUSTOMER [C_W_ID=%d, C_D_ID=%d, C_LAST=%s]", c_w_id, c_d_id, c_last);

            throw new RuntimeException(msg);
        }

        // TPC-C 2.5.2.2: Position n / 2 rounded up to the next integer, but
        // that counts starting from 1.
        int index = customers.size() / 2;
        if (customers.size() % 2 == 0) {
            index -= 1;
        }
        return customers.get(index);
    }


}




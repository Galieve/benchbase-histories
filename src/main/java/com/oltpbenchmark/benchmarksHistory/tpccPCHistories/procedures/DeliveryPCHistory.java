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
import com.oltpbenchmark.benchmarksHistory.tpccPCHistories.TPCCPCWorkerHistory;
import com.oltpbenchmark.historyModelHistory.events.*;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.NewOrderHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.OpenOrderHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo.OrderLineHistory;
import com.oltpbenchmark.utilHistory.SQLUtilHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.function.Function;

public class DeliveryPCHistory extends TPCCProcedureNDHistory {
    private static final Logger LOG = LoggerFactory.getLogger(DeliveryPCHistory.class);

    public SQLStmt delivGetOrderIdSQL = new SQLStmt(
        """
            SELECT * FROM %s
             WHERE NO_D_ID = ?
               AND NO_W_ID = ?
             ORDER BY NO_O_ID ASC
        """.formatted(TPCCConstantsHistory.TABLENAME_NEWORDER));

    public SQLStmt delivDeleteNewOrderSQL = new SQLStmt(
        """
            UPDATE %s
            SET NO_DELIVERED = ?,
                WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
            WHERE NO_O_ID = ?
            AND NO_D_ID = ?
            AND NO_W_ID = ?
            RETURNING *
        """.formatted(TPCCConstantsHistory.TABLENAME_NEWORDER));

    public SQLStmt delivGetCustIdSQL = new SQLStmt(
        """
            SELECT * FROM %s
            WHERE O_ID = ?
            AND O_D_ID = ?
            AND O_W_ID = ?
        """.formatted(TPCCConstantsHistory.TABLENAME_OPENORDER));

    public SQLStmt delivUpdateCarrierIdSQL = new SQLStmt(
        """
            UPDATE %s
             SET O_CARRIER_ID = ?, WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
             WHERE O_ID = ?
               AND O_D_ID = ?
               AND O_W_ID = ?
             RETURNING *
        """.formatted(TPCCConstantsHistory.TABLENAME_OPENORDER));

    public SQLStmt delivUpdateDeliveryDateSQL = new SQLStmt(
        """
            UPDATE %s
               SET OL_DELIVERY_D = ?,
                   WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
             WHERE OL_O_ID = ?
               AND OL_D_ID = ?
               AND OL_W_ID = ?
               RETURNING *
        """.formatted(TPCCConstantsHistory.TABLENAME_ORDERLINE));

    public SQLStmt delivSumOrderAmountSQL = new SQLStmt(
        """
            SELECT *
              FROM %s
             WHERE OL_O_ID = ?
               AND OL_D_ID = ?
               AND OL_W_ID = ?
        """.formatted(TPCCConstantsHistory.TABLENAME_ORDERLINE));

    public SQLStmt delivUpdateCustBalDelivCntSQL = new SQLStmt(
        """
            UPDATE %s
               SET C_BALANCE = C_BALANCE + ?,
                   C_DELIVERY_CNT = C_DELIVERY_CNT + 1,
                   WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))
             WHERE C_W_ID = ?
               AND C_D_ID = ?
               AND C_ID = ?
             RETURNING *
        """.formatted(TPCCConstantsHistory.TABLENAME_CUSTOMER));


    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, long startTime, TPCCPCWorkerHistory w, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;

        int o_carrier_id = TPCCUtilHistory.randomNumber(1, 10, gen);

        int d_id;

        int[] orderIDs = new int[10];

        for (d_id = 1; d_id <= terminalDistrictUpperID; d_id++) {
            Integer no_o_id = getOrderId(conn, w_id, d_id, id, soID, po, events);
            ++po;

            if (no_o_id == null) {
                continue;
            }

            orderIDs[d_id - 1] = no_o_id;

            deleteOrder(conn, w_id, d_id, no_o_id, id, soID, po, events);
            ++po;

            int customerId = getCustomerId(conn, w_id, d_id, no_o_id, id, soID, po, events);
            ++po;

            updateCarrierId(conn, w_id, o_carrier_id, d_id, no_o_id, id, soID, po, events);
            ++po;

            updateDeliveryDate(conn, w_id, d_id, no_o_id, id, soID, po, events);
            ++po;

            float orderLineTotal = getOrderLineTotal(conn, w_id, d_id, no_o_id, id, soID, po, events);
            ++po;

            updateBalanceAndDelivery(conn, w_id, d_id, customerId, orderLineTotal, id, soID, po, events);
            ++po;
        }

        if (LOG.isTraceEnabled()) {
            StringBuilder terminalMessage = new StringBuilder();
            terminalMessage.append("\n+---------------------------- DELIVERY ---------------------------+\n");
            terminalMessage.append(" Date: ");
            terminalMessage.append(TPCCUtilHistory.getCurrentTime());
            terminalMessage.append("\n\n WarehouseHistory: ");
            terminalMessage.append(w_id);
            terminalMessage.append("\n Carrier:   ");
            terminalMessage.append(o_carrier_id);
            terminalMessage.append("\n\n Delivered Orders\n");
            for (int i = 1; i <= TPCCConfigHistory.configDistPerWhse; i++) {
                if (orderIDs[i - 1] >= 0) {
                    terminalMessage.append("  DistrictHistory ");
                    terminalMessage.append(i < 10 ? " " : "");
                    terminalMessage.append(i);
                    terminalMessage.append(": Order number ");
                    terminalMessage.append(orderIDs[i - 1]);
                    terminalMessage.append(" was delivered.\n");
                }
            }
            terminalMessage.append("+-----------------------------------------------------------------+\n\n");
            LOG.trace(terminalMessage.toString());
        }

    }

    private Integer getOrderId(Connection conn, int w_id, int d_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        try (PreparedStatement delivGetOrderId = this.getPreparedStatement(conn, delivGetOrderIdSQL)) {
            delivGetOrderId.setInt(1, d_id);
            delivGetOrderId.setInt(2, w_id);

            try (ResultSet rs = delivGetOrderId.executeQuery()) {
                Integer ret = null;
                if (!rs.next()) {
                    // This district has no new orders.  This can happen but should be rare
                    LOG.warn(String.format("DistrictHistory has no new orders [W_ID=%d, D_ID=%d]", w_id, d_id));
                }
                else {
                    while(rs.next()){
                        if(!rs.getBoolean("NO_DELIVERED")){
                            ret = rs.getInt("NO_O_ID");
                            break;
                        }
                    }

                }
                var no = new NewOrderHistory();
                var p = no.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    String.valueOf(w_id).equals(val.getValue("NO_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("NO_D_ID"));
                events.add(new SelectEvent(id, so, po, p, where, no.getTableNames()));
                return ret;

            }
        }
    }

    private void deleteOrder(Connection conn, int w_id, int d_id, int no_o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        var writeID = EventID.generateID(id, so, po);

        try (PreparedStatement delivDeleteNewOrder = this.getPreparedStatement(conn, delivDeleteNewOrderSQL)) {
            delivDeleteNewOrder.setBoolean(1, true);
            delivDeleteNewOrder.setString(2, writeID);
            delivDeleteNewOrder.setInt(3, no_o_id);
            delivDeleteNewOrder.setInt(4, d_id);
            delivDeleteNewOrder.setInt(5, w_id);

            delivDeleteNewOrder.execute();
            var rs = delivDeleteNewOrder.getResultSet();

            int result = SQLUtilHistory.size(rs);

            var no = new NewOrderHistory();


            var p = no.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(no_o_id).equals(val.getValue("NO_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("NO_D_ID")) &&
                String.valueOf(w_id).equals(val.getValue("NO_O_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, no.getTableNames()));
            if (result != 1) {
                // This code used to run in a loop in an attempt to make this work
                // with MySQL's default weird consistency level. We just always run
                // this as SERIALIZABLE instead. I don't *think* that fixing this one
                // error makes this work with MySQL's default consistency.
                // Careful auditing would be required.
                String msg = String.format("NewOrderPCHistory delete failed. Not running with SERIALIZABLE isolation? [w_id=%d, d_id=%d, no_o_id=%d]", w_id, d_id, no_o_id);
                throw new UserAbortException(msg);
            }


        }
    }

    private int getCustomerId(Connection conn, int w_id, int d_id, int no_o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        try (PreparedStatement delivGetCustId = this.getPreparedStatement(conn, delivGetCustIdSQL)) {
            delivGetCustId.setInt(1, no_o_id);
            delivGetCustId.setInt(2, d_id);
            delivGetCustId.setInt(3, w_id);

            try (ResultSet rs = delivGetCustId.executeQuery()) {

                var oo = new OpenOrderHistory();
                var p = oo.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    String.valueOf(w_id).equals(val.getValue("O_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("O_D_ID")) &&
                    String.valueOf(no_o_id).equals(val.getValue("O_ID"));
                events.add(new SelectEvent(id, so, po, p, where, oo.getTableNames()));

                rs.beforeFirst();
                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }

                return rs.getInt("O_C_ID");
            }
        }
    }

    private void updateCarrierId(Connection conn, int w_id, int o_carrier_id, int d_id, int no_o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement delivUpdateCarrierId = this.getPreparedStatement(conn, delivUpdateCarrierIdSQL)) {
            var evID = EventID.generateID(id, so, po);
            delivUpdateCarrierId.setInt(1, o_carrier_id);
            delivUpdateCarrierId.setString(2, evID);

            delivUpdateCarrierId.setInt(3, no_o_id);
            delivUpdateCarrierId.setInt(4, d_id);
            delivUpdateCarrierId.setInt(5, w_id);

            delivUpdateCarrierId.execute();

            var rs = delivUpdateCarrierId.getResultSet();

            int result = SQLUtilHistory.size(rs);


            var oo = new OpenOrderHistory();
            var p = oo.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("O_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("O_D_ID")) &&
                String.valueOf(no_o_id).equals(val.getValue("O_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, oo.getTableNames()));

            if (result != 1) {
                String msg = String.format("Failed to update ORDER record [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                throw new RuntimeException(msg);
            }
        }
    }

    private void updateDeliveryDate(Connection conn, int w_id, int d_id, int no_o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());

        var evID = EventID.generateID(id, so, po);

        try (PreparedStatement delivUpdateDeliveryDate = this.getPreparedStatement(conn, delivUpdateDeliveryDateSQL)) {
            delivUpdateDeliveryDate.setTimestamp(1, timestamp);
            delivUpdateDeliveryDate.setString(2, evID);
            delivUpdateDeliveryDate.setInt(3, no_o_id);
            delivUpdateDeliveryDate.setInt(4, d_id);
            delivUpdateDeliveryDate.setInt(5, w_id);

            delivUpdateDeliveryDate.execute();

            var rs = delivUpdateDeliveryDate.getResultSet();


            int result = SQLUtilHistory.size(rs);



            var ol = new OrderLineHistory();
            var p = ol.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("OL_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("OL_D_ID")) &&
                String.valueOf(no_o_id).equals(val.getValue("OL_O_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, ol.getTableNames()));

            if (result == 0) {
                String msg = String.format("Failed to update ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                throw new RuntimeException(msg);
            }
        }
    }

    private float getOrderLineTotal(Connection conn, int w_id, int d_id, int no_o_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement delivSumOrderAmount = this.getPreparedStatement(conn, delivSumOrderAmountSQL)) {
            delivSumOrderAmount.setInt(1, no_o_id);
            delivSumOrderAmount.setInt(2, d_id);
            delivSumOrderAmount.setInt(3, w_id);

            try (ResultSet rs = delivSumOrderAmount.executeQuery()) {


                var ol = new OrderLineHistory();
                var p = ol.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    String.valueOf(w_id).equals(val.getValue("OL_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("OL_D_ID")) &&
                    String.valueOf(no_o_id).equals(val.getValue("OL_O_ID"));
                events.add(new SelectEvent(id, so, po, p, where, ol.getTableNames()));

                rs.beforeFirst();
                int sum = 0;
                if (!rs.next()) {
                    String msg = String.format("Failed to retrieve ORDER_LINE records [W_ID=%d, D_ID=%d, O_ID=%d]", w_id, d_id, no_o_id);
                    throw new RuntimeException(msg);
                }
                rs.beforeFirst();
                while(rs.next()){
                    sum += rs.getInt("OL_AMOUNT");
                }
                return sum;
            }
        }
    }

    private void updateBalanceAndDelivery(Connection conn, int w_id, int d_id, int c_id, float orderLineTotal, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {

        var evID = EventID.generateID(id, so, po);


        try (PreparedStatement delivUpdateCustBalDelivCnt = this.getPreparedStatement(conn, delivUpdateCustBalDelivCntSQL)) {
            delivUpdateCustBalDelivCnt.setBigDecimal(1, BigDecimal.valueOf(orderLineTotal));
            delivUpdateCustBalDelivCnt.setString(2, evID);
            delivUpdateCustBalDelivCnt.setInt(3, w_id);
            delivUpdateCustBalDelivCnt.setInt(4, d_id);
            delivUpdateCustBalDelivCnt.setInt(5, c_id);

            delivUpdateCustBalDelivCnt.execute();

            var rs = delivUpdateCustBalDelivCnt.getResultSet();

            int result = SQLUtilHistory.size(rs);

            var c = new CustomerHistory();

            var p = c.getUpdateEventInfo(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("C_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("C_D_ID")) &&
                String.valueOf(c_id).equals(val.getValue("C_ID"));
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, c.getTableNames()));
            if (result == 0) {
                String msg = String.format("Failed to update CUSTOMER record [W_ID=%d, D_ID=%d, C_ID=%d]", w_id, d_id, c_id);
                throw new RuntimeException(msg);
            }
        }
    }
}

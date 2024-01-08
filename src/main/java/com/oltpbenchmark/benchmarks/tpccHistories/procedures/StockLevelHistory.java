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

package com.oltpbenchmark.benchmarks.tpccHistories.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.SelectEvent;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCConstantsHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCUtilHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCWorkerHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.DistrictHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.OrderLineHistory;
import com.oltpbenchmark.benchmarks.tpccHistories.pojo.StockHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.function.Function;

public class StockLevelHistory extends TPCCProcedureHistory {

    private static final Logger LOG = LoggerFactory.getLogger(StockLevelHistory.class);

    public SQLStmt stockGetDistOrderIdSQL = new SQLStmt(
    """
        SELECT *
          FROM  %s
         WHERE D_W_ID = ?
           AND D_ID = ?
    """.formatted(TPCCConstantsHistory.TABLENAME_DISTRICT));

    public SQLStmt stockGetCountStockSQL = new SQLStmt(
    """
        SELECT COUNT(DISTINCT (S_I_ID)) AS STOCK_COUNT
         FROM  %s, %s
         WHERE OL_W_ID = ?
         AND OL_D_ID = ?
         AND OL_O_ID < ?
         AND OL_O_ID >= ?
         AND S_W_ID = ?
         AND S_I_ID = OL_I_ID
         AND S_QUANTITY < ?
    """.formatted(TPCCConstantsHistory.TABLENAME_ORDERLINE, TPCCConstantsHistory.TABLENAME_STOCK));

    public SQLStmt getOrderLineIdSQL = new SQLStmt(
        "SELECT * " +
        " FROM " + TPCCConstantsHistory.TABLENAME_ORDERLINE +
        " WHERE OL_W_ID=? " +
        " AND OL_D_ID=? " +
        " AND OL_O_ID < ? " +
        " AND OL_O_ID >= ?");

    public SQLStmt getCountStockSQL = new SQLStmt(
        "SELECT * " +
        " FROM " + TPCCConstantsHistory.TABLENAME_STOCK +
        " WHERE S_W_ID=? " +
        " AND S_I_ID = ?" +
        " AND S_QUANTITY < ? ");

    public void run(Connection conn, Random gen, int w_id, int numWarehouses, int terminalDistrictLowerID, int terminalDistrictUpperID, long startTime, TPCCWorkerHistory w, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;
        int threshold = TPCCUtilHistory.randomNumber(10, 20, gen);
        int d_id = TPCCUtilHistory.randomNumber(terminalDistrictLowerID, terminalDistrictUpperID, gen);

        int o_id = getOrderId(conn, w_id, d_id, id, soID, po, events);
        ++po;

        int stock_count = getStockCount(conn, w_id, threshold, d_id, o_id, id, soID, po, events);

        if (LOG.isTraceEnabled()) {
            String terminalMessage = "\n+-------------------------- STOCK-LEVEL --------------------------+" +
                                     "\n WarehouseHistory: " +
                                     w_id +
                                     "\n DistrictHistory:  " +
                                     d_id +
                                     "\n\n StockHistory Level Threshold: " +
                                     threshold +
                                     "\n Low StockHistory Count:       " +
                                     stock_count +
                                     "\n+-----------------------------------------------------------------+\n\n";
            LOG.trace(terminalMessage);
        }


    }

    private int getOrderId(Connection conn, int w_id, int d_id, Integer id, Integer so, Integer po, ArrayList<Event> events) throws SQLException {
        try (PreparedStatement stockGetDistOrderId = this.getPreparedStatement(conn, stockGetDistOrderIdSQL)) {
            stockGetDistOrderId.setInt(1, w_id);
            stockGetDistOrderId.setInt(2, d_id);

            try (ResultSet rs = stockGetDistOrderId.executeQuery()) {
                if (!rs.next()) {

                    throw new RuntimeException("D_W_ID=" + w_id + " D_ID=" + d_id + " not found!");
                }
                var d = new DistrictHistory();
                var p = d.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("D_W_ID")) &&
                    String.valueOf(d_id).equals(val.getValue("D_ID")) ;
                events.add(new SelectEvent(id, so, po, p, where, d.getTableNames()));

                rs.beforeFirst();
                rs.next();
                return rs.getInt("D_NEXT_O_ID");
            }
        }

    }

    private int getStockCount(Connection conn, int w_id, int threshold, int d_id, int o_id, int id, int so, int po, ArrayList<Event> events) throws SQLException {
        try(var getOrderLineId = this.getPreparedStatement(conn, getOrderLineIdSQL)){
            getOrderLineId.setInt(1, w_id);
            getOrderLineId.setInt(2, d_id);
            getOrderLineId.setInt(3, o_id);
            getOrderLineId.setInt(4, o_id - 20);

            var olRs = getOrderLineId.executeQuery();

            var ol = new OrderLineHistory();
            var p = ol.getSelectEventInfo(olRs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                String.valueOf(w_id).equals(val.getValue("OL_W_ID")) &&
                String.valueOf(d_id).equals(val.getValue("OL_D_ID")) &&
                Integer.parseInt(val.getValue("OL_O_ID")) < o_id &&
                Integer.parseInt(val.getValue("OL_O_ID")) >= o_id - 20;
            events.add(new SelectEvent(id, so, po, p, where, ol.getTableNames()));
            ++po;
            olRs.beforeFirst();

            var getCountStock = this.getPreparedStatement(conn, getCountStockSQL);

            Set<Integer> stockIds = new HashSet<>();
            while (olRs.next()) {
                int ol_i_id = olRs.getInt("OL_I_ID");
                getCountStock.setInt(1, w_id);
                getCountStock.setInt(2, ol_i_id);
                getCountStock.setInt(3, threshold);
                ResultSet sRs = getCountStock.executeQuery();

                var s = new StockHistory();
                p = s.getSelectEventInfo(sRs);
                where = (val) ->
                val != null &&
                    String.valueOf(w_id).equals(val.getValue("S_W_ID")) &&
                    String.valueOf(ol_i_id).equals(val.getValue("S_I_ID")) &&
                    Integer.parseInt(val.getValue("S_QUANTITY")) < threshold;
                events.add(new SelectEvent(id, so, po, p, where, s.getTableNames()));
                ++po;
                sRs.beforeFirst();

                while (sRs.next()) {
                    stockIds.add(sRs.getInt("S_I_ID"));

                }
            }
            return stockIds.size();
        }
    }
}

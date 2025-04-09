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


package com.oltpbenchmark.benchmarks.seatsHistories.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.apiHistory.ProcedureHistory;
import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.*;
import com.oltpbenchmark.utilHistory.SQLUtilHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class UpdateCustomerHistory extends ProcedureHistory {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateCustomerHistory.class);

    public final SQLStmt GetCustomerIdStr = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            " WHERE C_ID_STR = ? "
    );

    public final SQLStmt GetCustomer = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            " WHERE C_ID = ? "
    );

    public final SQLStmt GetBaseAirport = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_AIRPORT +
            " WHERE AP_ID = ?"
    );

    public final SQLStmt GetBaseCountry = new SQLStmt(
        "SELECT * " +
        "  FROM " +
        SEATSConstantsHistory.TABLENAME_COUNTRY +
        " WHERE CO_ID = ? "
    );

    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            "   SET C_IATTR00 = ?, " +
            "       C_IATTR01 = ?, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE C_ID = ? RETURNING *"
    );

    public final SQLStmt GetFrequentFlyers = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_FREQUENT_FLYER +
            " WHERE FF_C_ID = ?"
    );

    public final SQLStmt UpdateFrequentFlyers = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR00 = ?, " +
            "       FF_IATTR01 = ?, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ? " +
            "   RETURNING *"
    );

    public void run(Connection conn, String c_id, String c_id_str, Long update_ff, long attr0, long attr1, ArrayList<Event> events, int id, int so) throws SQLException {
        int po = 0;
        // Use C_ID_STR to get C_ID
        if (c_id == null) {


            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetCustomerIdStr, c_id_str)) {
                try (ResultSet rs = preparedStatement.executeQuery()) {

                    Function<Value, Boolean> where = (val) ->
                        val != null &&
                        val.getValue("C_ID_STR").equals(c_id_str);

                    var c = new Customer();
                    var wro = c.getSelectEventInfo(rs);
                    events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));
                    ++po;


                    rs.beforeFirst();
                    if (rs.next()) {
                        c_id = rs.getString("C_ID");
                    } else {
                        LOG.debug("No Customer information record found for string '{}'", c_id_str);
                        return;
                    }
                }
            }
        }

        long base_airport;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetCustomer, c_id)) {
            try (ResultSet rs = preparedStatement.executeQuery()) {

                String finalC_id = c_id;
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("C_ID").equals(finalC_id);

                var c = new Customer();
                var wro = c.getSelectEventInfo(rs);
                events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));
                rs.beforeFirst();
                if (!rs.next()) {
                    LOG.debug("No Customer information record found for id '{}'", c_id);
                    return;
                }

                base_airport = rs.getLong("C_BASE_AP_ID");
            }
        }
        ++po;

        long co_id;

        // Get their airport information
        // TODO: Do something interesting with this data
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetBaseAirport, base_airport)) {
            try (ResultSet airport_results = preparedStatement.executeQuery()) {

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    Long.parseLong(val.getValue("AP_ID")) == base_airport;

                var ai = new Airport();
                var wro = ai.getSelectEventInfo(airport_results);
                events.add(new SelectEvent(id, so, po, wro, where, ai.getTableNames()));

                airport_results.beforeFirst();
                if (!airport_results.next()) {
                    LOG.debug("No Airport information record found for id '{}'", base_airport);
                    return;
                }
                co_id = airport_results.getLong("AP_CO_ID");
            }
        }

        ++po;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetBaseCountry, co_id)) {
            try (ResultSet results = preparedStatement.executeQuery()) {
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    Long.parseLong(val.getValue("CO_ID")) == co_id;

                var co = new Country();
                var wro = co.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, co.getTableNames()));

                results.beforeFirst();
                results.next();

            }
        }

        ++po;


        long ff_al_id;

        if (update_ff != null) {
            try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetFrequentFlyers, c_id)) {
                try (ResultSet ff_results = preparedStatement.executeQuery()) {

                    String finalC_id1 = c_id;
                    Function<Value, Boolean> where = (val) ->
                        val != null &&
                        val.getValue("FF_C_ID").equals(finalC_id1);

                    var ff = new FrequentFlyer();
                    var wro = ff.getSelectEventInfo(ff_results);
                    events.add(new SelectEvent(id, so, po, wro, where, ff.getTableNames()));

                    ++po;

                    ff_results.beforeFirst();
                    while (ff_results.next()) {
                        ff_al_id = ff_results.getLong("FF_AL_ID");
                        var evID = EventID.generateID(id, so, po);
                        try (PreparedStatement updateStatement = this.getPreparedStatement(conn, UpdateFrequentFlyers, attr0, attr1, evID, c_id, ff_al_id)) {

                            updateStatement.execute();
                            var rs = updateStatement.getResultSet();

                            long finalFf_al_id = ff_al_id;
                            Function<Value, Boolean> whereUpdate = (val) ->
                                val != null &&
                                val.getValue("FF_C_ID").equals(finalC_id1) &&
                                Long.parseLong(val.getValue("FF_AL_ID")) == finalFf_al_id;

                            var ffU = new FrequentFlyer();
                            var p = ffU.getUpdateEventInfo(rs);
                            events.add(new UpdateEvent(id, so, po, p.first, p.second, whereUpdate, ffU.getTableNames()));

                            ++po;

                        }
                    }
                }
            }
        }

        //++po;


        int updated;
        var evID = EventID.generateID(id, so, po);
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, UpdateCustomer, attr0, attr1, evID, c_id)) {
            preparedStatement.execute();
            var rs = preparedStatement.getResultSet();
            updated = SQLUtilHistory.size(rs);
            String finalC_id2 = c_id;
            Function<Value, Boolean> whereUpdate = (val) ->
                val != null &&
                val.getValue("C_ID").equals(finalC_id2);

            var c = new Customer();
            var p = c.getUpdateEventInfo(rs);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, whereUpdate, c.getTableNames()));
        }
        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to update customer #%s - Updated %d records", ErrorType.VALIDITY_ERROR, c_id, updated));
        }
        ++po;

    }
}

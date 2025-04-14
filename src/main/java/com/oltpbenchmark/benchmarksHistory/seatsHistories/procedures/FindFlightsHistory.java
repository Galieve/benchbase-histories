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


package com.oltpbenchmark.benchmarksHistory.seatsHistories.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarksHistory.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarksHistory.seatsHistories.pojo.*;
import com.oltpbenchmark.historyModelHistory.ProcedureHistory;
import com.oltpbenchmark.historyModelHistory.events.Event;
import com.oltpbenchmark.historyModelHistory.events.SelectEvent;
import com.oltpbenchmark.historyModelHistory.events.Value;
import com.oltpbenchmark.benchmarksHistory.seatsHistories.pojo.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

public class FindFlightsHistory extends ProcedureHistory {
    private static final Logger LOG = LoggerFactory.getLogger(FindFlightsHistory.class);


    public final SQLStmt GetNearbyAirports = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstantsHistory.TABLENAME_AIRPORT_DISTANCE +
        " WHERE D_AP_ID0 = ? " +
        "   AND D_DISTANCE <= ? " +
        " ORDER BY D_DISTANCE ASC "
    );

    public final SQLStmt GetAirportInfo = new SQLStmt(
            "SELECT * " +
            " FROM " + SEATSConstantsHistory.TABLENAME_AIRPORT +
            " WHERE AP_ID = ? "
    );

    public final SQLStmt GetCountry = new SQLStmt(
        "SELECT * " +
        " FROM " +
        SEATSConstantsHistory.TABLENAME_COUNTRY +
        " WHERE CO_ID = ? "
    );

    private final SQLStmt GetAirline = new SQLStmt(
        "SELECT * " +
        " FROM " +
        SEATSConstantsHistory.TABLENAME_AIRLINE +
        " WHERE AL_ID = ?"
    );

    private final static String BaseGetFlights =
            "SELECT * " +
            " FROM " + SEATSConstantsHistory.TABLENAME_FLIGHT +
            " WHERE F_DEPART_AP_ID = ? " +
            "   AND F_DEPART_TIME >= ? AND F_DEPART_TIME <= ? " +
            "   AND F_ARRIVE_AP_ID IN (??)";

    public final SQLStmt GetFlights1 = new SQLStmt(BaseGetFlights, 1);
    public final SQLStmt GetFlights2 = new SQLStmt(BaseGetFlights, 2);
    public final SQLStmt GetFlights3 = new SQLStmt(BaseGetFlights, 3);

    public List<Object[]> run(Connection conn, long depart_aid, long arrive_aid, Timestamp start_date, Timestamp end_date, long distance, ArrayList<Event> events, int id, int so) throws SQLException {

        int po = 0;
        try {


            final List<Long> arrive_aids = new ArrayList<>();
            arrive_aids.add(arrive_aid);

            final List<Object[]> finalResults = new ArrayList<>();

            if (distance > 0) {
                // First get the nearby airports for the departure and arrival cities
                try (PreparedStatement nearby_stmt = this.getPreparedStatement(conn, GetNearbyAirports, depart_aid, distance)) {
                    try (ResultSet nearby_results = nearby_stmt.executeQuery()) {
                        while (nearby_results.next()) {
                            long aid = nearby_results.getLong("D_AP_ID1");
                            double aid_distance = nearby_results.getDouble("D_DISTANCE");

                            LOG.debug("DEPART NEARBY: {} distance={} miles", aid, aid_distance);

                            arrive_aids.add(aid);
                        }
                        Function<Value, Boolean> where = (val) ->
                            val != null &&
                            Long.parseLong(val.getValue("D_AP_ID0")) == depart_aid &&
                            Double.parseDouble(val.getValue("D_DISTANCE")) <= distance;
                        var ad = new AirportDistance();
                        var wro = ad.getSelectEventInfo(nearby_results);
                        events.add(new SelectEvent(id, so, po, wro, where, ad.getTableNames()));
                    }
                }
                ++po;
            }

            // H-Store doesn't support IN clauses, so we'll only get nearby flights to nearby arrival cities
            int num_nearby = arrive_aids.size();
            if (num_nearby > 0) {

                SQLStmt sqlStmt;
                if (num_nearby == 1) {
                    sqlStmt = GetFlights1;
                } else if (num_nearby == 2) {
                    sqlStmt = GetFlights2;
                } else {
                    sqlStmt = GetFlights3;
                }


                try (PreparedStatement f_stmt = this.getPreparedStatement(conn, sqlStmt)) {

                    var arrival_ap_id = new HashSet<Long>();

                    // Set Parameters
                    f_stmt.setLong(1, depart_aid);
                    f_stmt.setTimestamp(2, start_date);
                    f_stmt.setTimestamp(3, end_date);
                    for (int i = 0, cnt = Math.min(3, num_nearby); i < cnt; i++) {
                        f_stmt.setLong(4 + i, arrive_aids.get(i));
                        arrival_ap_id.add(arrive_aids.get(i));
                    }


                    // Process Result
                    try (ResultSet flightResults = f_stmt.executeQuery()) {

                        Function<Value, Boolean> where = (val) ->
                            val != null &&
                            Long.parseLong(val.getValue("F_DEPART_AP_ID")) == depart_aid &&
                            !Timestamp.valueOf(val.getValue("F_DEPART_TIME")).before(start_date) &&
                            !Timestamp.valueOf(val.getValue("F_DEPART_TIME")).after(end_date) &&
                            arrival_ap_id.contains(Long.parseLong(val.getValue("F_ARRIVE_AP_ID")));
                        var f = new Flight();
                        var wro = f.getSelectEventInfo(flightResults);
                        events.add(new SelectEvent(id, so, po, wro, where, f.getTableNames()));

                        ++po;
                        flightResults.beforeFirst();

                        var al = new Airline();
                        var airLineRS = new ArrayList<String>();
                        try (PreparedStatement al_stmt = this.getPreparedStatement(conn, GetAirline)) {
                            while (flightResults.next()) {
                                var alID = flightResults.getLong("F_AL_ID");
                                al_stmt.setLong(1, alID);
                                al_stmt.execute();
                                var rsAl = al_stmt.getResultSet();

                                Function<Value, Boolean> whereAl = (val) ->
                                    val != null &&
                                    Long.parseLong(val.getValue("AL_ID")) == alID;
                                var wroAl = al.getSelectEventInfo(rsAl);
                                events.add(new SelectEvent(id, so, po, wroAl, whereAl, al.getTableNames()));

                                rsAl.first();
                                //There is at most one result in the query!
                                airLineRS.add(rsAl.getString("AL_NAME"));

                                ++po;
                            }
                        }

                        flightResults.beforeFirst();

                        try (PreparedStatement ai_stmt = this.getPreparedStatement(conn, GetAirportInfo)) {

                            var co_stmt = this.getPreparedStatement(conn, GetCountry);
                            int i = 0;
                            while (flightResults.next()) {
                                long f_depart_airport = flightResults.getLong("F_DEPART_AP_ID");
                                long f_arrive_airport = flightResults.getLong("F_ARRIVE_AP_ID");

                                Object[] row = new Object[13];
                                int r = 0;

                                row[r++] = flightResults.getString("F_ID");    // [00] F_ID
                                row[r++] = flightResults.getLong("F_SEATS_LEFT");    // [01] SEATS_LEFT

                                row[r++] = airLineRS.get(i);  // [02] AL_NAME

                                // DEPARTURE AIRPORT
                                ai_stmt.setLong(1, f_depart_airport);
                                try (ResultSet ai_results = ai_stmt.executeQuery()) {

                                    Function<Value, Boolean> whereAi = (val) ->
                                        val != null &&
                                        Long.parseLong(val.getValue("AP_ID")) == f_depart_airport;
                                    var ai = new Airport();
                                    var wroAi = ai.getSelectEventInfo(ai_results);
                                    events.add(new SelectEvent(id, so, po, wroAi, whereAi, ai.getTableNames()));

                                    ++po;
                                    ai_results.first();

                                    row[r++] = flightResults.getDate("F_DEPART_TIME");    // [03] DEPART_TIME
                                    row[r++] = ai_results.getString("AP_CODE");     // [04] DEPART_AP_CODE
                                    row[r++] = ai_results.getString("AP_NAME");     // [05] DEPART_AP_NAME
                                    row[r++] = ai_results.getString("AP_CITY");     // [06] DEPART_AP_CITY

                                    var ap_co_id = ai_results.getLong("AP_CO_ID");
                                    co_stmt.setLong(1, ap_co_id);
                                    co_stmt.execute();
                                    var co_results = co_stmt.getResultSet();
                                    co_results.next();
                                    row[r++] = co_results.getString("CO_NAME");     // [07] DEPART_AP_COUNTRY

                                    Function<Value, Boolean> whereCo = (val) ->
                                        val != null &&
                                        Long.parseLong(val.getValue("CO_ID")) == ap_co_id;
                                    var co = new Country();
                                    var wroCo = co.getSelectEventInfo(co_results);
                                    events.add(new SelectEvent(id, so, po, wroCo, whereCo, co.getTableNames()));
                                    ++po;

                                }

                                // ARRIVAL AIRPORT
                                ai_stmt.setLong(1, f_arrive_airport);
                                try (ResultSet ai_results = ai_stmt.executeQuery()) {

                                    Function<Value, Boolean> whereAi = (val) ->
                                        val != null &&
                                        Long.parseLong(val.getValue("AP_ID")) == f_arrive_airport;
                                    var ai = new Airport();
                                    var wroAi = ai.getSelectEventInfo(ai_results);
                                    events.add(new SelectEvent(id, so, po, wroAi, whereAi, ai.getTableNames()));

                                    ++po;
                                    ai_results.first();

                                    row[r++] = flightResults.getDate("F_ARRIVE_TIME");    // [08] ARRIVE_TIME
                                    row[r++] = ai_results.getString("AP_CODE");     // [09] ARRIVE_AP_CODE
                                    row[r++] = ai_results.getString("AP_NAME");     // [10] ARRIVE_AP_NAME
                                    row[r++] = ai_results.getString("AP_CITY");     // [11] ARRIVE_AP_CITY

                                    var ap_co_id = ai_results.getLong("AP_CO_ID");
                                    co_stmt.setLong(1, ap_co_id);
                                    co_stmt.execute();
                                    var co_results = co_stmt.getResultSet();
                                    co_results.next();
                                    row[r] = co_results.getString("CO_NAME");     // [12] ARRIVE_AP_COUNTRY

                                    Function<Value, Boolean> whereCo = (val) ->
                                        val != null &&
                                        Long.parseLong(val.getValue("CO_ID")) == ap_co_id;
                                    var co = new Country();
                                    var wroCo = co.getSelectEventInfo(co_results);
                                    events.add(new SelectEvent(id, so, po, wroCo, whereCo, co.getTableNames()));
                                    ++po;
                                }

                                finalResults.add(row);

                            }
                        }
                    }
                }

            }

            LOG.debug("Flight Information:\n{}", finalResults);

            return (finalResults);
        } catch (SQLException esql) {
            LOG.error("caught SQLException in FindFlightsHistory:{}", esql, esql);
            throw esql;
        } catch (Exception e) {
            LOG.error("caught Exception in FindFlightsHistory:{}", e, e);
        }
        return null;
    }
}

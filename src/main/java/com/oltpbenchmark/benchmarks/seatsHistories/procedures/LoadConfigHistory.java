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
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.SelectEvent;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.*;
import com.oltpbenchmark.util.SQLUtil;
import com.oltpbenchmark.utilHistory.SQLUtilHistory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

public class LoadConfigHistory extends Procedure {

    // -----------------------------------------------------------------
    // STATEMENTS
    // -----------------------------------------------------------------

    public final SQLStmt getConfigProfile = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_CONFIG_PROFILE
    );

    public final SQLStmt getConfigHistogram = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_CONFIG_HISTOGRAMS
    );

    public final SQLStmt getCountryCodes = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_COUNTRY
    );

    public final SQLStmt getAirportCodes = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_AIRPORT
    );

    public final SQLStmt getAirlineCodes = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_AIRLINE +
            " WHERE AL_IATA_CODE != ''"
    );

    public final SQLStmt getFlights = new SQLStmt(
            "SELECT f_id FROM " + SEATSConstantsHistory.TABLENAME_FLIGHT +
            " ORDER BY F_DEPART_TIME DESC "
    );

    public Config run(Connection conn, ArrayList<Event> events, int id, int so) throws SQLException {

        List<Object[]> configProfile;

        int po = 0;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getConfigProfile)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                configProfile = SQLUtil.toList(resultSet);

                Function<Value, Boolean> where = Objects::nonNull;
                var cp = new ConfigProfile();
                var wro = cp.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, cp.getTableNames()));
            }
        }
        ++po;

        List<Object[]> histogram;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getConfigHistogram)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                histogram = SQLUtil.toList(resultSet);
                Function<Value, Boolean> where = Objects::nonNull;
                var ch = new ConfigHistograms();
                var wro = ch.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, ch.getTableNames()));
            }
        }
        ++po;

        List<Object[]> countryCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getCountryCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                var columnNames = Set.of("CO_ID", "CO_CODE_3");
                countryCodes = SQLUtilHistory.toList(resultSet, columnNames);

                Function<Value, Boolean> where = Objects::nonNull;
                var c = new Country();
                var wro = c.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));
            }
        }

        ++po;

        List<Object[]> airportCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getAirportCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {

                var columnNames = Set.of("AP_ID", "AP_CODE");
                airportCodes = SQLUtilHistory.toList(resultSet, columnNames);

                Function<Value, Boolean> where = Objects::nonNull;
                var a = new Airport();
                var wro = a.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, a.getTableNames()));
            }
        }
        ++po;

        List<Object[]> airlineCodes;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getAirlineCodes)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                var columnNames = Set.of("AL_ID", "AL_IATA_CODE");
                airlineCodes = SQLUtilHistory.toList(resultSet, columnNames);

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    !val.getValue("AL_IATA_CODE").equals("");
                var al = new Airline();
                var wro = al.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, al.getTableNames()));

            }
        }
        ++po;

        List<Object[]> flights;
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, getFlights)) {
            try (ResultSet resultSet = preparedStatement.executeQuery()) {
                var columnNames = Set.of("F_ID");
                flights = SQLUtilHistory.toList(resultSet, columnNames);

                Function<Value, Boolean> where = Objects::nonNull;
                var f = new Flight();
                var wro = f.getSelectEventInfo(resultSet);
                events.add(new SelectEvent(id, so, po, wro, where, f.getTableNames()));
            }
        }

        return new Config(configProfile, histogram, countryCodes, airportCodes, airlineCodes, flights);
    }
}

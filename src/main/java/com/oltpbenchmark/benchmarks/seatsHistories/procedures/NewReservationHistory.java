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
import com.oltpbenchmark.benchmarks.seats.util.CustomerId;
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

public class NewReservationHistory extends ProcedureHistory {
    private static final Logger LOG = LoggerFactory.getLogger(NewReservationHistory.class);

    public final SQLStmt GetFlight = new SQLStmt(
            "SELECT * FROM " + SEATSConstantsHistory.TABLENAME_FLIGHT +
            " WHERE F_ID = ?");

    public final SQLStmt GetAirline = new SQLStmt(
        "SELECT * FROM " +
        SEATSConstantsHistory.TABLENAME_AIRLINE +
        " WHERE AL_ID = ?"
    );

    public final SQLStmt GetCustomer = new SQLStmt(
            "SELECT *" +
            "  FROM " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            " WHERE C_ID = ? ");

    public final SQLStmt CheckSeat = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? and R_SEAT = ?");

    public final SQLStmt CheckCustomer = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? AND R_C_ID = ?");

    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT - 1, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE F_ID = ? RETURNING *");

    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            "   SET C_IATTR10 = C_IATTR10 + 1, " +
            "       C_IATTR11 = C_IATTR11 + 1, " +
            "       C_IATTR12 = ?, " +
            "       C_IATTR13 = ?, " +
            "       C_IATTR14 = ?, " +
            "       C_IATTR15 = ?, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE C_ID = ? RETURNING *");

    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 + 1, " +
            "       FF_IATTR11 = ?, " +
            "       FF_IATTR12 = ?, " +
            "       FF_IATTR13 = ?, " +
            "       FF_IATTR14 = ?, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE FF_C_ID = ? " +
            "   AND FF_AL_ID = ? RETURNING *");

    public final SQLStmt InsertReservation = new SQLStmt(
            "INSERT INTO " + SEATSConstantsHistory.TABLENAME_RESERVATION + " (" +
            "   R_ID, " +
            "   R_C_ID, " +
            "   R_F_ID, " +
            "   R_SEAT, " +
            "   R_PRICE, " +
            "   R_IATTR00, " +
            "   R_IATTR01, " +
            "   R_IATTR02, " +
            "   R_IATTR03, " +
            "   R_IATTR04, " +
            "   R_IATTR05, " +
            "   R_IATTR06, " +
            "   R_IATTR07, " +
            "   R_IATTR08, " +
            "   WRITEID " +
            ") VALUES (" +
            "   ?, " +  // R_ID
            "   ?, " +  // R_C_ID
            "   ?, " +  // R_F_ID
            "   ?, " +  // R_SEAT
            "   ?, " +  // R_PRICE
            "   ?, " +  // R_ATTR00
            "   ?, " +  // R_ATTR01
            "   ?, " +  // R_ATTR02
            "   ?, " +  // R_ATTR03
            "   ?, " +  // R_ATTR04
            "   ?, " +  // R_ATTR05
            "   ?, " +  // R_ATTR06
            "   ?, " +  // R_ATTR07
            "   ?, " +   // R_ATTR08
            "   ? " +   //WRITEID
            ") RETURNING *");

    public void run(Connection conn, long r_id, String c_id, String f_id, long seatnum, double price, long[] attrs, ArrayList<Event> events, int id, int so) throws SQLException {
        boolean found;

        long airline_id;
        long seats_left;
        int po = 0;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetFlight, f_id)) {
            try (ResultSet results = stmt.executeQuery()) {


                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("F_ID").equals(f_id);
                var f = new Flight();
                var wro = f.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, f.getTableNames()));

                results.beforeFirst();
                found = results.next();
                if (!found) {
                    LOG.debug("Error Type [{}]: Invalid flight {}", ErrorType.INVALID_FLIGHT_ID, f_id);
                    return;
                }
                airline_id = results.getLong("F_AL_ID");
                seats_left = results.getLong("F_SEATS_LEFT");
            }
        }
        ++po;

        // Airline Information
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetAirline, airline_id)) {
            try (ResultSet results = stmt.executeQuery()) {

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    Long.parseLong(val.getValue("AL_ID")) == airline_id;
                var al = new Airline();
                var wro = al.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, al.getTableNames()));

                results.beforeFirst();
                found = results.next();
                if (!found) {
                    LOG.debug("Error Type [{}]: Invalid airline {}", ErrorType.INVALID_FLIGHT_ID, airline_id);
                    return;
                }
            }
        }
        ++po;
        if (seats_left <= 0) {
            LOG.debug("Error Type [{}]: No more seats available for flight {}", ErrorType.NO_MORE_SEATS, f_id);
            return;
        }
        // Check if Seat is Available
        try (PreparedStatement stmt = this.getPreparedStatement(conn, CheckSeat, f_id, seatnum)) {
            try (ResultSet results = stmt.executeQuery()) {
                found = results.next();

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("R_F_ID").equals(f_id) &&
                    Long.parseLong(val.getValue("R_SEAT")) == seatnum;
                var r = new Reservation();
                var wro = r.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, r.getTableNames()));

            }
        }
        ++po;
        if (found) {
            LOG.debug("Error Type [{}]: Seat {} is already reserved on flight {}", ErrorType.SEAT_ALREADY_RESERVED, seatnum, f_id);
            return;
        }
        // Check if the Customer already has a seat on this flight
        try (PreparedStatement stmt = this.getPreparedStatement(conn, CheckCustomer, f_id, c_id)) {
            try (ResultSet results = stmt.executeQuery()) {
                found = results.next();

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("R_F_ID").equals(f_id) &&
                    val.getValue("R_C_ID").equals(c_id);
                var r = new Reservation();
                var wro = r.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, r.getTableNames()));
            }
        }
        ++po;
        if (found) {
            LOG.debug("Error Type [{}]: Customer {} already owns on a reservations on flight {}", ErrorType.CUSTOMER_ALREADY_HAS_SEAT, c_id, f_id);
            return;
        }
        // Get Customer Information
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, GetCustomer, c_id)) {
            try (ResultSet results = preparedStatement.executeQuery()) {

                found = results.next();

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("C_ID").equals(c_id);
                var c = new Customer();
                var wro = c.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));

            }
        }
        ++po;

        if (!found) {
            LOG.debug("Error Type [{}]: Invalid customer id: {} / {}", ErrorType.INVALID_CUSTOMER_ID, c_id, new CustomerId(c_id));
            return;
        }

        int updated;

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, InsertReservation)) {
            var writeID = EventID.generateID(id, so, po);

            preparedStatement.setLong(1, r_id);
            preparedStatement.setString(2, c_id);
            preparedStatement.setString(3, f_id);
            preparedStatement.setLong(4, seatnum);
            preparedStatement.setDouble(5, price);
            for (int i = 0; i < attrs.length; i++) {
                preparedStatement.setLong(6 + i, attrs[i]);
            }
            preparedStatement.setString(6 + attrs.length, writeID);

            preparedStatement.execute();

            var results = preparedStatement.getResultSet();
            updated = SQLUtilHistory.size(results);

            var r = new Reservation();
            var writeSet = r.getInsertEventInfo(results);
            events.add(new InsertEvent(id, so, po, writeSet, r.getTableNames()));
        }
        ++po;

        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to add reservation for flight #%s - Inserted %d records for InsertReservation", ErrorType.VALIDITY_ERROR, f_id, updated));
        }

        var writeIDflight = EventID.generateID(id, so, po);

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, UpdateFlight, writeIDflight, f_id)) {
            preparedStatement.execute();

            var results = preparedStatement.getResultSet();
            updated = SQLUtilHistory.size(results);

            var f = new Flight();
            var p = f.getUpdateEventInfo(results);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                val.getValue("F_ID").equals(f_id);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, f.getTableNames()));
        }
        ++po;

        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to add reservation for flight #%s - Updated %d records for UpdateFlight", ErrorType.VALIDITY_ERROR, f_id, updated));
        }

        var writeIDCustomer = EventID.generateID(id, so, po);

        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, UpdateCustomer, attrs[0], attrs[1], attrs[2], attrs[3], writeIDCustomer, c_id)) {
            preparedStatement.execute();

            var results = preparedStatement.getResultSet();
            updated = SQLUtilHistory.size(results);

            var c = new Customer();
            var p = c.getUpdateEventInfo(results);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                val.getValue("C_ID").equals(c_id);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, c.getTableNames()));
        }
        ++po;

        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to add reservation for flight #%s - Updated %d records for UpdateCustomerHistory", ErrorType.VALIDITY_ERROR, f_id, updated));
        }

        var writeIDFreqFlyer = EventID.generateID(id, so, po);

        // We don't care if we updated FrequentFlyer
        try (PreparedStatement preparedStatement = this.getPreparedStatement(conn, UpdateFrequentFlyer, attrs[4], attrs[5], attrs[6], attrs[7], writeIDFreqFlyer, c_id, airline_id)) {
            preparedStatement.execute();

            var results = preparedStatement.getResultSet();
            updated = SQLUtilHistory.size(results);

            var ff = new FrequentFlyer();
            var p = ff.getUpdateEventInfo(results);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                val.getValue("FF_C_ID").equals(c_id) &&
                Long.parseLong(val.getValue("FF_AL_ID")) == airline_id;
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, ff.getTableNames()));
        }
        ++po;


        LOG.debug(String.format("Reserved new seat on flight %s for customer %s [seatsLeft=%d]",
                f_id, c_id, seats_left - 1));


    }
}

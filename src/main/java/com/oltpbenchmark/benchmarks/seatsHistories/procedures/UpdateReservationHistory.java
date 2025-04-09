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

/* This file is part of VoltDB.
 * Copyright (C) 2009 Vertica Systems Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package com.oltpbenchmark.benchmarks.seatsHistories.procedures;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.apiHistory.ProcedureHistory;
import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.benchmarks.seats.util.ErrorType;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.Reservation;
import com.oltpbenchmark.utilHistory.SQLUtilHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class UpdateReservationHistory extends ProcedureHistory {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateReservationHistory.class);

    public final SQLStmt CheckSeat = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? and R_SEAT = ?");

    public final SQLStmt CheckCustomer = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
            " WHERE R_F_ID = ? AND R_C_ID = ?");

    private static final String BASE_SQL =
        "UPDATE " +
        SEATSConstantsHistory.TABLENAME_RESERVATION +
        "   SET R_SEAT = ?, " +
        "       %s = ?, " +
        "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
        " WHERE R_ID = ? " +
        "    AND R_C_ID = ?" +
        "    AND R_F_ID = ?" +
        " RETURNING *"
        ;

    public final SQLStmt ReserveSeat0 = new SQLStmt(String.format(BASE_SQL, "R_IATTR00"));
    public final SQLStmt ReserveSeat1 = new SQLStmt(String.format(BASE_SQL, "R_IATTR01"));
    public final SQLStmt ReserveSeat2 = new SQLStmt(String.format(BASE_SQL, "R_IATTR02"));
    public final SQLStmt ReserveSeat3 = new SQLStmt(String.format(BASE_SQL, "R_IATTR03"));

    public static final int NUM_UPDATES = 4;
    public final SQLStmt[] ReserveSeats = {
            ReserveSeat0,
            ReserveSeat1,
            ReserveSeat2,
            ReserveSeat3,
    };

    public void run(Connection conn, long r_id, String f_id, String c_id, long seatnum, long attr_idx, long attr_val, ArrayList<Event> events, int id, int so) throws SQLException {

        boolean found;
        int po = 0;

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

        if (!found) {
            LOG.debug("Error Type [{}]: Customer {} does not have an existing reservation on flight {}", ErrorType.CUSTOMER_ALREADY_HAS_SEAT, c_id, f_id);
            return;
        }

        // Update the seat reservation for the customer
        int updated;
        var evID = EventID.generateID(id, so, po);
        try (PreparedStatement stmt = this.getPreparedStatement(conn, ReserveSeats[(int) attr_idx], seatnum, attr_val, evID, r_id, c_id, f_id)) {
            stmt.execute();
            var rs = stmt.getResultSet();
            updated = SQLUtilHistory.size(rs);
            Function<Value, Boolean> where = (val) ->
                val != null &&
                Long.parseLong(val.getValue("R_ID")) == r_id &&
                val.getValue("R_F_ID").equals(f_id) &&
                val.getValue("R_C_ID").equals(c_id);

            var r = new Reservation();
            var p = r.getUpdateEventInfo(rs);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, r.getTableNames()));

        }
        ++po;

        if (updated != 1) {
            throw new UserAbortException(String.format("Error Type [%s]: Failed to update reservation on flight %s for customer #%s - Updated %d records", ErrorType.VALIDITY_ERROR, f_id, c_id, updated));
        }


        LOG.debug(String.format("Updated reservation on flight %s for customer %s", f_id, c_id));

    }
}

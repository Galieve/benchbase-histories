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
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.SelectEvent;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.Reservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class FindOpenSeatsHistory extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(FindOpenSeatsHistory.class);

    public final SQLStmt GetFlight = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstantsHistory.TABLENAME_FLIGHT +
        " WHERE F_ID = ?"
    );

    public final SQLStmt GetSeats = new SQLStmt(
        "SELECT * " +
        "  FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
        " WHERE R_F_ID = ?"
    );

    public Object[][] run(Connection conn, String f_id, ArrayList<Event> events, int id, int so) throws SQLException {

        int po = 0;

        // 150 seats
        final long[] seatmap = new long[]
                {-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                        -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1};


        double base_price = 0.0;
        long seats_total = 0;
        long seats_left = 0;
        double seat_price = 0.0;

        // First calculate the seat price using the flight's base price
        // and the number of seats that remaining
        try (PreparedStatement f_stmt = this.getPreparedStatement(conn, GetFlight)) {
            f_stmt.setString(1, f_id);
            try (ResultSet f_results = f_stmt.executeQuery()) {
                if (f_results.next()) {

                    // long status = results[0].getLong(0);
                    base_price = f_results.getDouble("F_BASE_PRICE");
                    seats_total = f_results.getLong("F_SEATS_TOTAL");
                    seats_left = f_results.getLong("F_SEATS_LEFT");
                    seat_price = base_price + (base_price * (1.0- seats_left /(double) seats_total));
                } else {
                    LOG.warn("flight {} had no seats; this may be a data problem or a code problem.  previously this threw an unhandled exception.", f_id);
                }
            }
        }

        ++po;
        // TODO: Figure out why this doesn't match the SQL
        //   Possible explanation: Floating point numbers are approximations;
        //                         there is no exact representation of (for example) 0.01.
        //                         Some databases (like PostgreSQL) will use exact types,
        //                         such as numeric, for intermediate values.  (This is
        //                         more-or-less equivalent to java.math.BigDecimal.)
        double _seat_price = base_price + (base_price * (1.0 - (seats_left / (double) seats_total)));

        LOG.debug(String.format("Flight %s - SQL[%.2f] <-> JAVA[%.2f] [basePrice=%f, total=%d, left=%d]",
                f_id, seat_price, _seat_price, base_price, seats_total, seats_left));


        // Then build the seat map of the remaining seats
        try (PreparedStatement s_stmt = this.getPreparedStatement(conn, GetSeats)) {
            s_stmt.setString(1, f_id);
            try (ResultSet s_results = s_stmt.executeQuery()) {
                while (s_results.next()) {
                    long r_id = s_results.getLong("R_ID");
                    int seatnum = s_results.getInt("R_SEAT");

                    LOG.debug(String.format("Reserved Seat: fid %s / rid %d / seat %d", f_id, r_id, seatnum));

                    seatmap[seatnum] = 1;
                }

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("R_F_ID").equals(f_id);
                var r = new Reservation();
                var wro = r.getSelectEventInfo(s_results);
                events.add(new SelectEvent(id, so, po, wro, where, r.getTableNames()));


            }
        }
        ++po;

        int ctr = 0;
        Object[][] returnResults = new Object[SEATSConstantsHistory.FLIGHTS_NUM_SEATS][];
        for (int i = 0; i < seatmap.length; ++i) {
            if (seatmap[i] == -1) {
                // Charge more for the first seats
                double price = seat_price * (i < SEATSConstantsHistory.FLIGHTS_FIRST_CLASS_OFFSET ? 2.0 : 1.0);
                Object[] row = new Object[]{f_id, i, price};
                returnResults[ctr++] = row;
                if (ctr == returnResults.length) {
                    break;
                }
            }
        }

        return returnResults;
    }

}

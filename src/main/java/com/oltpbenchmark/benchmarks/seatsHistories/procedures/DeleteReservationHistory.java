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
import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.Customer;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.Flight;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.FrequentFlyer;
import com.oltpbenchmark.benchmarks.seatsHistories.pojo.Reservation;
import com.oltpbenchmark.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.function.Function;

public class DeleteReservationHistory extends Procedure {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteReservationHistory.class);

    public final SQLStmt GetCustomerByIdStr = new SQLStmt(
            "SELECT * " +
            "  FROM " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            " WHERE C_ID_STR = ?");

    public final SQLStmt GetFFNumber = new SQLStmt(
        "SELECT * FROM " +
        SEATSConstantsHistory.TABLENAME_FREQUENT_FLYER +
        " WHERE FF_C_ID_STR = ?");

    public final SQLStmt GetCustomerByFF = new SQLStmt(
        "SELECT * " +
        " FROM " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
        " WHERE C_ID = ?");

    public final SQLStmt GetSeatsLeft = new SQLStmt(
        "SELECT *" +
        "  FROM " +
        SEATSConstantsHistory.TABLENAME_FLIGHT +
        " WHERE F_ID = ? "
    );

    public final SQLStmt GetCIAttr00 = new SQLStmt(
        "SELECT *" +
        "  FROM " +
        SEATSConstantsHistory.TABLENAME_CUSTOMER +
        " WHERE C_ID = ? "
    );

    public final SQLStmt GetReservation = new SQLStmt(
        "SELECT *" +
        "  FROM " +
        SEATSConstantsHistory.TABLENAME_RESERVATION +
        " WHERE R_C_ID = ? AND R_F_ID = ?"
    );

    public final SQLStmt DeleteReservation = new SQLStmt(
            "DELETE FROM " + SEATSConstantsHistory.TABLENAME_RESERVATION +
            " WHERE R_ID = ? AND R_C_ID = ? AND R_F_ID = ? RETURNING *");

    public final SQLStmt UpdateFlight = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_FLIGHT +
            "   SET F_SEATS_LEFT = F_SEATS_LEFT + 1 ," +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE F_ID = ? RETURNING *");

    public final SQLStmt UpdateCustomer = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_CUSTOMER +
            "   SET C_BALANCE = C_BALANCE + ?, " +
            "       C_IATTR00 = ?, " +
            "       C_IATTR10 = C_IATTR10 - 1, " +
            "       C_IATTR11 = C_IATTR10 - 1, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE C_ID = ? ");

    public final SQLStmt UpdateFrequentFlyer = new SQLStmt(
            "UPDATE " + SEATSConstantsHistory.TABLENAME_FREQUENT_FLYER +
            "   SET FF_IATTR10 = FF_IATTR10 - 1, " +
            "       WRITEID = CONCAT(?, ';', SPLIT_PART(WRITEID, ';', 1))" +
            " WHERE FF_C_ID = ?" +
            "   AND FF_AL_ID = ?");

    public void run(Connection conn, String f_id, String c_id, String c_id_str, String ff_c_id_str, Long ff_al_id, ArrayList<Event> events, int id, int so) throws SQLException {

        int po = 0;


        // If we weren't given the customer id, then look it up
        if (c_id == null) {


            // Use the customer's id as a string
            if (c_id_str != null && c_id_str.length() > 0) {
                c_id = getCustomerById(conn, c_id_str, events, id, so, po);
            }
            // Otherwise use their FrequentFlyer information
            else {
                var p = getCustomerByFFNumber(conn, ff_c_id_str, true, events, id, so, po);
                c_id = p.first;
                ff_al_id = p.second;
            }

            ++po;
            if(c_id == null){
                LOG.debug("No Customer record was found [c_id_str={}, ff_c_id_str={}, ff_al_id={}]", c_id_str, ff_c_id_str, ff_al_id);
                return;
            }

        }

        // Now get the result of the information that we need
        // If there is no valid customer record, then throw an abort
        // This should happen 5% of the time

        Long c_iattr00;
        Long seats_left;
        Long r_id;
        Double r_price;

        seats_left = getSeatsLeft(conn, f_id, events, id, so, po);
        ++po;
        c_iattr00 = getCIAttr00(conn, c_id, events, id, so, po);
        ++po;
        var reservation = getReservationIDPrice(conn, c_id, f_id, events, id, so, po);
        r_id = reservation.first;
        r_price = reservation.second;

        ++po;

        if(seats_left == null || c_iattr00 == null || r_id == null
            || r_price == null) return;



        // Now delete all of the flights that they have on this flight
        try (PreparedStatement stmt = this.getPreparedStatement(conn, DeleteReservation, r_id, c_id, f_id)) {
            stmt.execute();
            var rs = stmt.getResultSet();
            String finalC_id = c_id;
            Function<Value, Boolean> where = (val) ->
                val != null &&
                Long.parseLong(val.getValue("R_ID")) == r_id &&
                val.getValue("R_C_ID").equals(finalC_id) &&
                val.getValue("R_F_ID").equals(f_id);
            var r = new Reservation();
            var p = r.getDeleteEventInfo(rs);
            events.add(new DeleteEvent(id, so, po, p.first, p.second, where, r.getTableNames()));

        }

        ++po;


        // Update Available Seats on Flight
        try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateFlight)) {
            var evID = EventID.generateID(id, so, po);
            stmt.setString(1, evID);
            stmt.setString(2, f_id);
            stmt.execute();
            var rs = stmt.getResultSet();
            Function<Value, Boolean> where = (val) ->
                val != null &&
                val.getValue("R_F_ID").equals(f_id);
            /*
            Function<Value, Value> set = (val)->{
                val.setValue("F_SEATS_LEFT", String.valueOf(Long.parseLong(val.getValue("F_SEATS_LEFT")) + 1));
                val.setValue("WRITEID", evID);
                return val;
            };
             */
            var r = new Reservation();
            var p = r.getUpdateEventInfo(rs);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, r.getTableNames()));
        }

        ++po;

        // Update Customer's Balance
        try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateCustomer)) {
            var evID = EventID.generateID(id, so, po);

            var balanceDelta = BigDecimal.valueOf(-1 * r_price);
            stmt.setBigDecimal(1, balanceDelta);
            stmt.setLong(2, c_iattr00);
            stmt.setString(3, evID);
            stmt.setString(4, c_id);
            stmt.execute();
            var rs = stmt.getResultSet();
            String finalC_id1 = c_id;
            Function<Value, Boolean> where = (val) ->
                val != null &&
                val.getValue("C_ID").equals(finalC_id1);
            /*
            Function<Value, Value> set = (val)->{
                val.setValue("C_BALANCE", String.valueOf(Double.parseDouble(val.getValue("C_BALANCE")) + balanceDelta.doubleValue()));
                val.setValue("C_IATTR00", String.valueOf(c_iattr00));
                val.setValue("C_IATTR10", String.valueOf(Long.parseLong(val.getValue("C_IATTR10")) - 1));
                val.setValue("C_IATTR11", String.valueOf(Long.parseLong(val.getValue("C_IATTR10")) - 1));
                val.setValue("WRITEID", evID);
                return val;
            };
             */
            var r = new Reservation();
            var p = r.getUpdateEventInfo(rs);
            events.add(new UpdateEvent(id, so, po, p.first, p.second, where, r.getTableNames()));
        }

        ++po;

        // Update Customer's Frequent Flyer Information (Optional)
        if (ff_al_id != null) {
            try (PreparedStatement stmt = this.getPreparedStatement(conn, UpdateFrequentFlyer, c_id, ff_al_id)) {
                var evID = EventID.generateID(id, so, po);

                stmt.setString(1,evID);
                stmt.setString(2, c_id);
                stmt.setLong(3, ff_al_id);
                stmt.execute();
                var rs = stmt.getResultSet();

                String finalC_id = c_id;
                Long finalFf_al_id = ff_al_id;

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("FF_C_ID").equals(finalC_id) &&
                    Long.parseLong(val.getValue("FF_AL_ID")) == finalFf_al_id;

                /*
                Function<Value, Value> set = (val)->{
                    val.setValue("FF_IATTR10", String.valueOf(Long.parseLong(val.getValue("FF_IATTR10")) -1 ));
                    val.setValue("WRITEID", evID);
                    return val;
                };
                */

                var r = new Reservation();
                var p = r.getUpdateEventInfo(rs);
                events.add(new UpdateEvent(id, so, po, p.first, p.second, where, r.getTableNames()));
            }
        }

        LOG.debug(String.format("Deleted reservation on flight %s for customer %s [seatsLeft=%d]", f_id, c_id, seats_left + 1));

    }

    private Pair<Long, Double> getReservationIDPrice(Connection conn, String cID, String fID, ArrayList<Event> events, int id, int so, int po) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetReservation)) {
            stmt.setString(1, cID);
            stmt.setString(2, fID);
            Long r_id;
            Double r_price;
            try (ResultSet results = stmt.executeQuery()) {
                if (!results.next()) {
                    LOG.debug("No Reservation information record found for id '{} {}'", cID, fID);
                    r_id = null;
                    r_price = null;
                }
                else{
                    r_id = results.getLong("R_ID");
                    r_price = results.getDouble("R_PRICE");
                }

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("R_C_ID").equals(cID) &&
                    val.getValue("R_F_ID").equals(fID);
                var r = new Reservation();
                var wro = r.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, r.getTableNames()));
                return new Pair<>(r_id, r_price);
            }
        }
    }

    private Long getCIAttr00(Connection conn, String cID, ArrayList<Event> events, int id, int so, int po) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetCIAttr00)) {
            stmt.setString(1, cID);
            try (ResultSet results = stmt.executeQuery()) {
                Long c_iattr00 = null;
                if (!results.next()) {
                    LOG.debug("No Customer information record found for id '{}'", cID);
                    c_iattr00 = null;
                }
                else {
                    c_iattr00 = results.getLong("C_IATTR00") + 1;
                }

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("C_ID").equals(cID);
                var c = new Customer();
                var wro = c.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));
                return c_iattr00;
            }
        }
    }

    private Long getSeatsLeft(Connection conn, String fID, ArrayList<Event> events, int id, int so, int po) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetSeatsLeft)) {
            stmt.setString(1, fID);
            try (ResultSet results = stmt.executeQuery()) {
                Long seats_left = null;
                if (!results.next()) {
                    LOG.debug("No Flight information record found for id '{}'", fID);
                    seats_left = null;
                }
                else {
                    seats_left = results.getLong("F_SEATS_LEFT");
                }

                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("F_ID").equals(fID);
                var f = new Flight();
                var wro = f.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, f.getTableNames()));
                return seats_left;
            }
        }

    }

    private Pair<String, Long> getCustomerByFFNumber(Connection conn, String ffCIdStr, boolean b, ArrayList<Event> events, int id, int so, int po) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetFFNumber, ffCIdStr)) {
            try (ResultSet resultsFF = stmt.executeQuery()) {
                String ffCId;
                Long ffAlId;
                if (resultsFF.next()) {
                    ffCId = resultsFF.getString("FF_C_ID");
                    ffAlId = resultsFF.getLong("FF_AL_ID");
                } else {
                    ffCId = null;
                    ffAlId = null;
                }
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("FF_C_ID_STR").equals(ffCIdStr);
                var ff = new FrequentFlyer();
                var wro = ff.getSelectEventInfo(resultsFF);
                events.add(new SelectEvent(id, so, po, wro, where, ff.getTableNames()));

                if(ffCId == null) return new Pair<>(null, null);

                String cID;
                PreparedStatement stmtCust = this.getPreparedStatement(conn, GetCustomerByFF, ffCId);
                ResultSet results = stmtCust.executeQuery();
                if (results.next()) {
                    cID = results.getString("C_ID");
                } else {
                    cID = null;
                }

                Function<Value, Boolean> whereC = (val) ->
                    val != null &&
                    val.getValue("C_ID").equals(ffCId);
                var c = new Customer();
                var wroC = c.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wroC, whereC, c.getTableNames()));

                return new Pair<>(cID, ffAlId);
            }
        }

    }

    private String getCustomerById(Connection conn, String cIdStr, ArrayList<Event> events, int id, int so, int po) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, GetCustomerByIdStr, cIdStr)) {
            try (ResultSet results = stmt.executeQuery()) {
                String cId;
                if (results.next()) {
                    cId = results.getString("C_ID");
                } else {
                    cId = null;
                }
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    val.getValue("C_ID_STR").equals(cIdStr);
                var c = new Customer();
                var wro = c.getSelectEventInfo(results);
                events.add(new SelectEvent(id, so, po, wro, where, c.getTableNames()));
                return cId;
            }
        }

    }

}

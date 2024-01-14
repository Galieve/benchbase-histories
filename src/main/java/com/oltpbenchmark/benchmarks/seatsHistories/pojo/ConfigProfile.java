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


package com.oltpbenchmark.benchmarks.seatsHistories.pojo;

import com.oltpbenchmark.apiHistory.PojoHistory;
import com.oltpbenchmark.benchmarks.seatsHistories.SEATSConstantsHistory;
import com.oltpbenchmark.util.Pair;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ConfigProfile implements PojoHistory {

    public float cfp_scale_factor;
    public String cfp_airport_max_customer;

    public Timestamp cfp_flight_start;
    public Timestamp cfp_flight_upcoming;
    public int cfp_flight_past_days;
    public int cfp_flight_future_days;

    public int cfp_flight_offset;
    public int cfp_reservation_offset;
    public long cfp_num_reservations;
    public String cfp_code_ids_xrefs;


    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
        );
    }

    /*

    public float cfp_scale_factor;
    public String cfp_airport_max_customer;

    public Timestamp cfp_flight_start;
    public Timestamp cfp_flight_upcoming;
    public int cfp_flight_past_days;
    public int cfp_flight_future_days;

    public int cfp_flight_offset;
    public int cfp_reservation_offset;
    public long cfp_num_reservations;
    public String cfp_code_ids_xrefs;
     */
    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("FLOAT", "CFP_SCALE_FACTOR"),
            new Pair<>("STRING", "CFP_AIRPORT_MAX_CUSTOMER"),
            new Pair<>("TIMESTAMP", "CFP_FLIGHT_START"),
            new Pair<>("TIMESTAMP", "CFP_FLIGHT_UPCOMING"),
            new Pair<>("INT", "CFP_FLIGHT_PAST_DAYS"),
            new Pair<>("INT", "CFP_FLIGHT_FUTURE_DAYS"),
            new Pair<>("INT", "CFP_FLIGHT_OFFSET"),
            new Pair<>("INT", "CFP_RESERVATION_OFFSET"),
            new Pair<>("LONG", "CFP_NUM_RESERVATIONS"),
            new Pair<>("STRING", "CFP_CODE_IDS_XREFS"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(SEATSConstantsHistory.TABLENAME_CONFIG_PROFILE));
        }
        return table;
    }
}

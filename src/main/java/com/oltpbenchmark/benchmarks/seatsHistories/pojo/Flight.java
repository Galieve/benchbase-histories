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

public class Flight implements PojoHistory {

    public String f_id;

    public long f_al_id;
    public long f_depart_ap_id;
    public Timestamp f_depart_time;
    public long f_arrive_ap_id;
    public Timestamp f_arrive_time;
    public long f_status;
    public float f_base_price;
    public long f_seats_total;
    public long f_seats_left;

    public long f_iattr00;
    public long f_iattr01;
    public long f_iattr02;
    public long f_iattr03;
    public long f_iattr04;
    public long f_iattr05;
    public long f_iattr06;
    public long f_iattr07;
    public long f_iattr08;
    public long f_iattr09;
    public long f_iattr10;
    public long f_iattr11;
    public long f_iattr12;
    public long f_iattr13;
    public long f_iattr14;
    public long f_iattr15;
    public long f_iattr16;
    public long f_iattr17;
    public long f_iattr18;
    public long f_iattr19;
    public long f_iattr20;
    public long f_iattr21;
    public long f_iattr22;
    public long f_iattr23;
    public long f_iattr24;
    public long f_iattr25;
    public long f_iattr26;
    public long f_iattr27;
    public long f_iattr28;
    public long f_iattr29;
    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("STRING", "F_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("STRING", "F_ID"),
            new Pair<>("LONG", "F_AL_ID"),
            new Pair<>("LONG", "F_DEPART_AP_ID"),
            new Pair<>("TIMESTAMP", "F_DEPART_TIME"),
            new Pair<>("LONG", "F_ARRIVE_AP_ID"),
            new Pair<>("TIMESTAMP", "F_ARRIVE_TIME"),
            new Pair<>("LONG", "F_STATUS"),
            new Pair<>("FLOAT", "F_BASE_PRICE"),
            new Pair<>("LONG", "F_SEATS_TOTAL"),
            new Pair<>("LONG", "F_SEATS_LEFT"),
            new Pair<>("LONG", "F_IATTR00"),
            new Pair<>("LONG", "F_IATTR01"),
            new Pair<>("LONG", "F_IATTR02"),
            new Pair<>("LONG", "F_IATTR03"),
            new Pair<>("LONG", "F_IATTR04"),
            new Pair<>("LONG", "F_IATTR05"),
            new Pair<>("LONG", "F_IATTR06"),
            new Pair<>("LONG", "F_IATTR07"),
            new Pair<>("LONG", "F_IATTR08"),
            new Pair<>("LONG", "F_IATTR09"),
            new Pair<>("LONG", "F_IATTR10"),
            new Pair<>("LONG", "F_IATTR11"),
            new Pair<>("LONG", "F_IATTR12"),
            new Pair<>("LONG", "F_IATTR13"),
            new Pair<>("LONG", "F_IATTR14"),
            new Pair<>("LONG", "F_IATTR15"),
            new Pair<>("LONG", "F_IATTR16"),
            new Pair<>("LONG", "F_IATTR17"),
            new Pair<>("LONG", "F_IATTR18"),
            new Pair<>("LONG", "F_IATTR19"),
            new Pair<>("LONG", "F_IATTR20"),
            new Pair<>("LONG", "F_IATTR21"),
            new Pair<>("LONG", "F_IATTR22"),
            new Pair<>("LONG", "F_IATTR23"),
            new Pair<>("LONG", "F_IATTR24"),
            new Pair<>("LONG", "F_IATTR25"),
            new Pair<>("LONG", "F_IATTR26"),
            new Pair<>("LONG", "F_IATTR27"),
            new Pair<>("LONG", "F_IATTR28"),
            new Pair<>("LONG", "F_IATTR29"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(this.getClass().getName()));
        }
        return table;
    }
}

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


package com.oltpbenchmark.benchmarksHistory.seatsHistories.pojo;

import com.oltpbenchmark.historyModelHistory.PojoHistory;
import com.oltpbenchmark.util.Pair;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Reservation implements PojoHistory {

    public long r_id;

    public String r_c_id;

    public String r_f_id;

    public long r_seat;

    public float r_price;

    public long r_iattr00;
    public long r_iattr01;
    public long r_iattr02;
    public long r_iattr03;
    public long r_iattr04;
    public long r_iattr05;
    public long r_iattr06;
    public long r_iattr07;
    public long r_iattr08;
    public long r_iattr09;

    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("LONG", "R_ID"),
            new Pair<>("STRING", "R_C_ID"),
            new Pair<>("STRING", "R_F_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("LONG", "R_ID"),
            new Pair<>("STRING", "R_C_ID"),
            new Pair<>("STRING", "R_F_ID"),
            new Pair<>("LONG", "R_SEAT"),
            new Pair<>("FLOAT", "R_PRICE"),
            new Pair<>("LONG", "R_IATTR00"),
            new Pair<>("LONG", "R_IATTR01"),
            new Pair<>("LONG", "R_IATTR02"),
            new Pair<>("LONG", "R_IATTR03"),
            new Pair<>("LONG", "R_IATTR04"),
            new Pair<>("LONG", "R_IATTR05"),
            new Pair<>("LONG", "R_IATTR06"),
            new Pair<>("LONG", "R_IATTR07"),
            new Pair<>("LONG", "R_IATTR08"),
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

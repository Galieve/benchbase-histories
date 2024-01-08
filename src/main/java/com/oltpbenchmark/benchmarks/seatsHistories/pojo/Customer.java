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
import com.oltpbenchmark.benchmarks.twitterHistories.TwitterConstantsHistory;
import com.oltpbenchmark.util.Pair;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Customer implements PojoHistory {

    public String c_id;

    public String c_id_str;

    public long c_base_ap_id;

    public float c_balance;

    public String c_sattr00;
    public String c_sattr01;
    public String c_sattr02;
    public String c_sattr03;
    public String c_sattr04;
    public String c_sattr05;
    public String c_sattr06;
    public String c_sattr07;

    public String c_sattr08;
    public String c_sattr09;
    public String c_sattr10;
    public String c_sattr11;
    public String c_sattr12;
    public String c_sattr13;
    public String c_sattr14;
    public String c_sattr15;
    public String c_sattr16;
    public String c_sattr17;
    public String c_sattr18;
    public String c_sattr19;

    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("STRING", "C_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("STRING", "C_ID"),
            new Pair<>("STRING", "C_ID_STR"),
            new Pair<>("STRING", "TEXT"),
            new Pair<>("TIMESTAMP", "CREATEDATE"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(TwitterConstantsHistory.TABLENAME_TWEETS));
        }
        return table;
    }
}

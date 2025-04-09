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

    public long c_iattr00;
    public long c_iattr01;
    public long c_iattr02;
    public long c_iattr03;
    public long c_iattr04;
    public long c_iattr05;
    public long c_iattr06;
    public long c_iattr07;
    public long c_iattr08;
    public long c_iattr09;
    public long c_iattr10;
    public long c_iattr11;
    public long c_iattr12;
    public long c_iattr13;
    public long c_iattr14;
    public long c_iattr15;
    public long c_iattr16;
    public long c_iattr17;
    public long c_iattr18;
    public long c_iattr19;

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
            new Pair<>("LONG", "C_BASE_AP_ID"),
            new Pair<>("FLOAT", "C_BALANCE"),
            new Pair<>("STRING", "C_SATTR00"),
            new Pair<>("STRING", "C_SATTR01"),
            new Pair<>("STRING", "C_SATTR02"),
            new Pair<>("STRING", "C_SATTR03"),
            new Pair<>("STRING", "C_SATTR04"),
            new Pair<>("STRING", "C_SATTR05"),
            new Pair<>("STRING", "C_SATTR06"),
            new Pair<>("STRING", "C_SATTR07"),
            new Pair<>("STRING", "C_SATTR08"),
            new Pair<>("STRING", "C_SATTR09"),
            new Pair<>("STRING", "C_SATTR10"),
            new Pair<>("STRING", "C_SATTR11"),
            new Pair<>("STRING", "C_SATTR12"),
            new Pair<>("STRING", "C_SATTR13"),
            new Pair<>("STRING", "C_SATTR14"),
            new Pair<>("STRING", "C_SATTR15"),
            new Pair<>("STRING", "C_SATTR16"),
            new Pair<>("STRING", "C_SATTR17"),
            new Pair<>("STRING", "C_SATTR18"),
            new Pair<>("STRING", "C_SATTR19"),
            new Pair<>("LONG", "C_IATTR00"),
            new Pair<>("LONG", "C_IATTR01"),
            new Pair<>("LONG", "C_IATTR02"),
            new Pair<>("LONG", "C_IATTR03"),
            new Pair<>("LONG", "C_IATTR04"),
            new Pair<>("LONG", "C_IATTR05"),
            new Pair<>("LONG", "C_IATTR06"),
            new Pair<>("LONG", "C_IATTR07"),
            new Pair<>("LONG", "C_IATTR08"),
            new Pair<>("LONG", "C_IATTR09"),
            new Pair<>("LONG", "C_IATTR10"),
            new Pair<>("LONG", "C_IATTR11"),
            new Pair<>("LONG", "C_IATTR12"),
            new Pair<>("LONG", "C_IATTR13"),
            new Pair<>("LONG", "C_IATTR14"),
            new Pair<>("LONG", "C_IATTR15"),
            new Pair<>("LONG", "C_IATTR16"),
            new Pair<>("LONG", "C_IATTR17"),
            new Pair<>("LONG", "C_IATTR18"),
            new Pair<>("LONG", "C_IATTR19"),
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

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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class FrequentFlyer implements PojoHistory {

    public String ff_c_id;

    public long ff_al_id;

    public String ff_c_id_str;

    public String ff_sattr00;
    public String ff_sattr01;
    public String ff_sattr02;
    public String ff_sattr03;

    public long ff_iattr00;
    public long ff_iattr01;
    public long ff_iattr02;
    public long ff_iattr03;
    public long ff_iattr04;
    public long ff_iattr05;
    public long ff_iattr06;
    public long ff_iattr07;
    public long ff_iattr08;
    public long ff_iattr09;
    public long ff_iattr10;
    public long ff_iattr11;
    public long ff_iattr12;
    public long ff_iattr13;
    public long ff_iattr14;
    public long ff_iattr15;

    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("STRING", "FF_C_ID"),
            new Pair<>("LONG", "FF_AL_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("STRING", "FF_C_ID"),
            new Pair<>("LONG", "FF_AL_ID"),
            new Pair<>("STRING", "FF_C_ID_STR"),
            new Pair<>("STRING", "FF_SATTR00"),
            new Pair<>("STRING", "FF_SATTR01"),
            new Pair<>("STRING", "FF_SATTR02"),
            new Pair<>("STRING", "FF_SATTR03"),
            new Pair<>("LONG", "FF_IATTR00"),
            new Pair<>("LONG", "FF_IATTR01"),
            new Pair<>("LONG", "FF_IATTR02"),
            new Pair<>("LONG", "FF_IATTR03"),
            new Pair<>("LONG", "FF_IATTR04"),
            new Pair<>("LONG", "FF_IATTR05"),
            new Pair<>("LONG", "FF_IATTR06"),
            new Pair<>("LONG", "FF_IATTR07"),
            new Pair<>("LONG", "FF_IATTR08"),
            new Pair<>("LONG", "FF_IATTR09"),
            new Pair<>("LONG", "FF_IATTR10"),
            new Pair<>("LONG", "FF_IATTR11"),
            new Pair<>("LONG", "FF_IATTR12"),
            new Pair<>("LONG", "FF_IATTR13"),
            new Pair<>("LONG", "FF_IATTR14"),
            new Pair<>("LONG", "FF_IATTR15"),
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

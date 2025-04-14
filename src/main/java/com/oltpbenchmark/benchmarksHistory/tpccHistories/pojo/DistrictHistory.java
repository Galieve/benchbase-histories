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


package com.oltpbenchmark.benchmarksHistory.tpccHistories.pojo;

import com.oltpbenchmark.historyModelHistory.PojoHistory;
import com.oltpbenchmark.benchmarks.tpcc.pojo.District;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class DistrictHistory extends District implements PojoHistory {


    public String d_writeID;

    protected static Set<String> table;


    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       d_writeID = " + d_writeID);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "D_W_ID"),
            new Pair<>("INT", "D_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "D_W_ID"),
            new Pair<>("INT", "D_ID"),
            new Pair<>("FLOAT", "D_YTD"),
            new Pair<>("FLOAT", "D_TAX"),
            new Pair<>("INT", "D_NEXT_O_ID"),
            new Pair<>("STRING", "D_NAME"),
            new Pair<>("STRING", "D_STREET_1"),
            new Pair<>("STRING", "D_STREET_2"),
            new Pair<>("STRING", "D_CITY"),
            new Pair<>("STRING", "D_STATE"),
            new Pair<>("STRING", "D_ZIP"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(this.getClass().getName()));
        }
        return table;
    }
}

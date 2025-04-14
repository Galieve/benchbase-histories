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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Stock;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class StockHistory extends Stock implements PojoHistory{

    public String s_writeID;

    protected static Set<String> table;


    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       s_writeID = " + s_writeID);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "S_W_ID"),
            new Pair<>("INT", "S_I_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "S_W_ID"),
            new Pair<>("INT", "S_I_ID"),
            new Pair<>("INT", "S_QUANTITY"),
            new Pair<>("FLOAT", "S_YTD"),
            new Pair<>("INT", "S_ORDER_CNT"),
            new Pair<>("INT", "S_REMOTE_CNT"),
            new Pair<>("STRING", "S_DATA"),
            new Pair<>("STRING", "S_DIST_01"),
            new Pair<>("STRING", "S_DIST_02"),
            new Pair<>("STRING", "S_DIST_03"),
            new Pair<>("STRING", "S_DIST_04"),
            new Pair<>("STRING", "S_DIST_05"),
            new Pair<>("STRING", "S_DIST_06"),
            new Pair<>("STRING", "S_DIST_07"),
            new Pair<>("STRING", "S_DIST_08"),
            new Pair<>("STRING", "S_DIST_09"),
            new Pair<>("STRING", "S_DIST_10"),
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

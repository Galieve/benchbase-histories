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
import com.oltpbenchmark.benchmarks.tpcc.pojo.OrderLine;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class OrderLineHistory extends OrderLine implements PojoHistory {

    public String ol_writeID;

    protected static Set<String> table;

    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       ol_writeID = " + ol_writeID);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "OL_W_ID"),
            new Pair<>("INT", "OL_D_ID"),
            new Pair<>("INT", "OL_O_ID"),
            new Pair<>("INT", "OL_NUMBER")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "OL_W_ID"),
            new Pair<>("INT", "OL_D_ID"),
            new Pair<>("INT", "OL_O_ID"),
            new Pair<>("INT", "OL_NUMBER"),
            new Pair<>("INT", "OL_I_ID"),
            new Pair<>("TIMESTAMP", "OL_DELIVERY_D"),
            new Pair<>("FLOAT", "OL_AMOUNT"),
            new Pair<>("INT", "OL_SUPPLY_W_ID"),
            new Pair<>("FLOAT", "OL_QUANTITY"),
            new Pair<>("STRING", "OL_DIST_INFO"),
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

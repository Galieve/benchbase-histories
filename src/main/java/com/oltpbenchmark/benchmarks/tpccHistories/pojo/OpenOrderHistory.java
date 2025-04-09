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


package com.oltpbenchmark.benchmarks.tpccHistories.pojo;

import com.oltpbenchmark.apiHistory.PojoHistory;
import com.oltpbenchmark.benchmarks.tpcc.pojo.Oorder;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class OpenOrderHistory extends Oorder  implements PojoHistory {

    public String o_writeID;

    protected static Set<String> table;

    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       o_writeID = " + o_writeID);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "O_W_ID"),
            new Pair<>("INT", "O_D_ID"),
            new Pair<>("INT", "O_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "O_W_ID"),
            new Pair<>("INT", "O_D_ID"),
            new Pair<>("INT", "O_ID"),
            new Pair<>("INT", "O_C_ID"),
            new Pair<>("INT", "O_CARRIER_ID"),
            new Pair<>("INT", "O_OL_CNT"),
            new Pair<>("INT", "O_ALL_LOCAL"),
            new Pair<>("TIMESTAMP", "O_ENTRY_D"),
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

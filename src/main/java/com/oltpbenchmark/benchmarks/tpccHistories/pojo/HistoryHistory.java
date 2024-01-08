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
import com.oltpbenchmark.benchmarks.tpcc.pojo.History;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class HistoryHistory extends History  implements PojoHistory {


    public String h_writeID;

    protected static Set<String> table;


    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       h_writeID = " + h_writeID);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "H_C_ID"),
            new Pair<>("INT", "H_C_W_ID"),
            new Pair<>("INT", "H_C_D_ID"),
            new Pair<>("INT", "H_D_ID"),
            new Pair<>("INT", "H_W_ID"),
            new Pair<>("TIMESTAMP", "H_DATE"),
            new Pair<>("FLOAT", "H_AMOUNT"),
            new Pair<>("STRING", "H_DATA"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "H_C_ID"),
            new Pair<>("INT", "H_C_W_ID"),
            new Pair<>("INT", "H_C_D_ID"),
            new Pair<>("INT", "H_D_ID"),
            new Pair<>("INT", "H_W_ID"),
            new Pair<>("TIMESTAMP", "H_DATE"),
            new Pair<>("FLOAT", "H_AMOUNT"),
            new Pair<>("STRING", "H_DATA"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton("History"));
        }
        return table;
    }

}

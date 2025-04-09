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


package com.oltpbenchmark.benchmarks.tpccPCHistories.pojo;

import com.oltpbenchmark.benchmarks.tpccHistories.pojo.NewOrderHistory;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class NewOrderPCHistory extends NewOrderHistory {
    public Boolean no_delivered;

    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 3,"*       no_delivered = " + no_delivered);
        return String.join("\n", spl);
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        var vl = super.getValuesList();
        vl.add(new Pair<>("BOOLEAN", "NO_DELIVERED"));
        return vl;
    }

    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(this.getClass().getName()));
        }
        return table;
    }
}

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


package com.oltpbenchmark.benchmarks.twitterHistories.pojo;

import com.oltpbenchmark.apiHistory.PojoHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.TwitterConstantsHistory;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class User implements PojoHistory {

    public long uid;

    public String name;

    public String email;
    public int partitionid;
    public int partitionid2;

    public int followers;
    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "UID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "UID"),
            new Pair<>("STRING", "NAME"),
            new Pair<>("STRING", "EMAIL"),
            new Pair<>("INT", "PARTITIONID"),
            new Pair<>("INT", "PARTITIONID2"),
            new Pair<>("INT", "FOLLOWERS"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(TwitterConstantsHistory.TABLENAME_USER));
        }
        return table;
    }
}

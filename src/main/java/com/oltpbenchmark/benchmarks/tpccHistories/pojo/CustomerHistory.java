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
import com.oltpbenchmark.benchmarks.tpcc.pojo.Customer;
import com.oltpbenchmark.util.Pair;

import java.util.*;

public class CustomerHistory extends Customer implements PojoHistory {
    public String c_writeID;

    protected static Set<String> table;

    @Override
    public String toString() {
        var sup = super.toString();
        var spl = Arrays.asList(sup.split("\n"));
        spl.add(spl.size() - 2,"*       c_writeID = " + c_writeID);
        return String.join("\n", spl);
    }

    @Override
    public boolean equals(Object o) {
        if(!super.equals(o)) return false;
        CustomerHistory customer = (CustomerHistory) o;
        return customer.c_writeID.equals(c_writeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), c_writeID);
    }

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("INT", "C_W_ID"),
            new Pair<>("INT", "C_D_ID"),
            new Pair<>("INT", "C_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("INT", "C_W_ID"),
            new Pair<>("INT", "C_D_ID"),
            new Pair<>("INT", "C_ID"),
            new Pair<>("FLOAT", "C_DISCOUNT"),
            new Pair<>("STRING", "C_CREDIT"),
            new Pair<>("STRING", "C_LAST"),
            new Pair<>("STRING", "C_FIRST"),
            new Pair<>("FLOAT", "C_CREDIT_LIM"),
            new Pair<>("FLOAT", "C_BALANCE"),
            new Pair<>("FLOAT", "C_YTD_PAYMENT"),
            new Pair<>("INT", "C_PAYMENT_CNT"),
            new Pair<>("INT", "C_DELIVERY_CNT"),
            new Pair<>("STRING", "C_STREET_1"),
            new Pair<>("STRING", "C_STREET_2"),
            new Pair<>("STRING", "C_CITY"),
            new Pair<>("STRING", "C_STATE"),
            new Pair<>("STRING", "C_ZIP"),
            new Pair<>("STRING", "C_PHONE"),
            new Pair<>("TIMESTAMP", "C_SINCE"),
            new Pair<>("STRING", "C_MIDDLE"),
            new Pair<>("STRING", "C_DATA"),
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

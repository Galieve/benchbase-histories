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

public class Airline implements PojoHistory {

    public long al_id;

    public String al_iata_code;
    public String al_icao_code;
    public String al_call_sign;
    public String al_name;
    public long al_co_id;

    public long ap_iattr00;
    public long ap_iattr01;
    public long ap_iattr02;
    public long ap_iattr03;
    public long ap_iattr04;
    public long ap_iattr05;
    public long ap_iattr06;
    public long ap_iattr07;
    public long ap_iattr08;
    public long ap_iattr09;
    public long ap_iattr10;
    public long ap_iattr11;
    public long ap_iattr12;
    public long ap_iattr13;
    public long ap_iattr14;
    public long ap_iattr15;
    public String writeID;

    protected static Set<String> table;

    @Override
    public List<Pair<String, String>> getPKsList() {
        return List.of(
            new Pair<>("LONG", "AL_ID")
        );
    }

    @Override
    public List<Pair<String, String>> getValuesList() {
        return List.of(
            new Pair<>("LONG", "AL_ID"),
            new Pair<>("STRING", "AL_IATA_CODE"),
            new Pair<>("STRING", "AL_ICAO_CODE"),
            new Pair<>("STRING", "AL_CALL_SIGN"),
            new Pair<>("STRING", "AL_NAME"),
            new Pair<>("LONG", "AL_CO_ID"),
            new Pair<>("LONG", "AP_IATTR00"),
            new Pair<>("LONG", "AP_IATTR01"),
            new Pair<>("LONG", "AP_IATTR02"),
            new Pair<>("LONG", "AP_IATTR03"),
            new Pair<>("LONG", "AP_IATTR04"),
            new Pair<>("LONG", "AP_IATTR05"),
            new Pair<>("LONG", "AP_IATTR06"),
            new Pair<>("LONG", "AP_IATTR07"),
            new Pair<>("LONG", "AP_IATTR08"),
            new Pair<>("LONG", "AP_IATTR09"),
            new Pair<>("LONG", "AP_IATTR10"),
            new Pair<>("LONG", "AP_IATTR11"),
            new Pair<>("LONG", "AP_IATTR12"),
            new Pair<>("LONG", "AP_IATTR13"),
            new Pair<>("LONG", "AP_IATTR14"),
            new Pair<>("LONG", "AP_IATTR15"),
            new Pair<>("STRING", "WRITEID")
        );
    }

    @Override
    public Set<String> getTableNames() {
        if(table == null){
            table = new HashSet<>(Collections.singleton(SEATSConstantsHistory.TABLENAME_AIRPORT));
        }
        return table;
    }
}
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

package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.LoaderThread;
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * A LoaderThread is responsible for loading some portion of a
 * benchmark's database.
 * Note that each LoaderThread has its own database Connection handle.
 */
public abstract class LoaderThreadHistory extends LoaderThread {

    protected ArrayList<Event> events;
    public LoaderThreadHistory(BenchmarkModule benchmarkModule) {
        super(benchmarkModule);
        events = new ArrayList<>();
    }

    public ArrayList<Event> getEvents() {
        return events;
    }
}

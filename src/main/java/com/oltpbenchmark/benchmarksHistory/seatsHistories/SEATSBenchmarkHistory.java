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

package com.oltpbenchmark.benchmarksHistory.seatsHistories;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.historyModelHistory.BenchmarkModuleHistory;
import com.oltpbenchmark.benchmarksHistory.seatsHistories.procedures.LoadConfigHistory;
import com.oltpbenchmark.catalog.Table;
import com.oltpbenchmark.util.RandomGenerator;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SEATSBenchmarkHistory extends BenchmarkModuleHistory {

    private final RandomGenerator rng = new RandomGenerator((int) System.currentTimeMillis());

    public SEATSBenchmarkHistory(WorkloadConfiguration workConf) {
        super(workConf);
        this.registerSupplementalProcedure(LoadConfigHistory.class);
    }

    public String getDataDir() {
        return "/benchmarks/" + getBenchmarkName();
    }

    public RandomGenerator getRandomGenerator() {
        return (this.rng);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (LoadConfigHistory.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 1; i <= this.workConf.getTerminals(); ++i) {
            workers.add(new SEATSWorkerHistory(this, i));
        }
        return (workers);
    }

    @Override
    protected Loader<SEATSBenchmarkHistory> makeLoaderImplHistory(int so) {
        return new SEATSLoaderHistory(this, so);
    }

    /**
     * Return the path of the CSV file that has data for the given Table catalog
     * handle
     *
     * @param data_dir
     * @param catalog_tbl
     * @return
     */
    public static String getTableDataFilePath(String data_dir, Table catalog_tbl) {
        return String.format("%s%stable.%s.csv", data_dir, File.separator, catalog_tbl.getName().toLowerCase());
    }


}

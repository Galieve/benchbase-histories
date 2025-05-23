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


package com.oltpbenchmark.benchmarksHistory.tpccHistories;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.*;
import com.oltpbenchmark.historyModelHistory.BenchmarkModuleHistory;
import com.oltpbenchmark.benchmarksHistory.tpccHistories.procedures.NewOrderHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TPCCBenchmarkHistory extends BenchmarkModuleHistory {
    private static final Logger LOG = LoggerFactory.getLogger(TPCCBenchmarkHistory.class);

    public TPCCBenchmarkHistory(WorkloadConfiguration workConf) {
        super(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return (NewOrderHistory.class.getPackage());
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() {
        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();

        try {
            List<TPCCWorkerHistory> terminals = createTerminals();
            workers.addAll(terminals);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return workers;
    }

    @Override
    protected Loader<TPCCBenchmarkHistory> makeLoaderImplHistory(int so) {
        return new TPCCLoaderHistory(this, so);
    }

    protected List<TPCCWorkerHistory> createTerminals() throws SQLException {

        TPCCWorkerHistory[] terminals = new TPCCWorkerHistory[workConf.getTerminals()];

        int numWarehouses = (int) workConf.getScaleFactor();
        if (numWarehouses <= 0) {
            numWarehouses = 1;
        }

        int numTerminals = workConf.getTerminals();

        // We distribute terminals evenly across the warehouses
        // Eg. if there are 10 terminals across 7 warehouses, they
        // are distributed as
        // 1, 1, 2, 1, 2, 1, 2
        final double terminalsPerWarehouse = (double) numTerminals / numWarehouses;
        int workerId = 1;

        for (int w = 0; w < numWarehouses; w++) {
            // Compute the number of terminals in *this* warehouse
            int lowerTerminalId = (int) (w * terminalsPerWarehouse);
            int upperTerminalId = (int) ((w + 1) * terminalsPerWarehouse);
            // protect against double rounding errors
            int w_id = w + 1;
            if (w_id == numWarehouses) {
                upperTerminalId = numTerminals;
            }
            int numWarehouseTerminals = upperTerminalId - lowerTerminalId;

            if (LOG.isDebugEnabled()) {
                LOG.debug(String.format("w_id %d = %d terminals [lower=%d / upper%d]", w_id, numWarehouseTerminals, lowerTerminalId, upperTerminalId));
            }

            final double districtsPerTerminal = TPCCConfigHistory.configDistPerWhse / (double) numWarehouseTerminals;
            for (int terminalId = 0; terminalId < numWarehouseTerminals; terminalId++) {
                int lowerDistrictId = (int) (terminalId * districtsPerTerminal);
                int upperDistrictId = (int) ((terminalId + 1) * districtsPerTerminal);
                if (terminalId + 1 == numWarehouseTerminals) {
                    upperDistrictId = TPCCConfigHistory.configDistPerWhse;
                }
                lowerDistrictId += 1;

                TPCCWorkerHistory terminal = new TPCCWorkerHistory(this, workerId++, w_id, lowerDistrictId, upperDistrictId, numWarehouses);
                terminals[lowerTerminalId + terminalId] = terminal;
            }

        }


        return Arrays.asList(terminals);
    }


}

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

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.*;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.apiHistory.isolationLevels.Serializabilty;
import com.oltpbenchmark.benchmarks.tpccHistories.TPCCBenchmarkHistory;
import com.oltpbenchmark.util.ClassUtil;
import com.oltpbenchmark.util.ThreadUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Base class for all benchmark implementations
 */
public abstract class BenchmarkModuleHistory extends BenchmarkModule{

    /**
     * Constructor!
     *
     * @param workConf
     */

    protected ArrayList<Transaction> loadTransactions;

    //auxiliar variable
    private Integer so;

    //We keep it as a variable for avoiding computing it twice
    protected Map<TransactionType, Procedure> procedures;

    public BenchmarkModuleHistory(WorkloadConfiguration workConf) {
        super(workConf);
        loadTransactions = null;
    }


    public final TransactionType initTransactionType(String procName, int id, long preExecutionWait, long postExecutionWait, Integer isolationLevel) {
        if (id == TransactionType.INVALID_ID) {
            throw new RuntimeException(String.format("Procedure %s.%s cannot use the reserved id '%d' for %s", getBenchmarkName(), procName, id, TransactionType.INVALID.getClass().getSimpleName()));
        }

        Package pkg = this.getProcedurePackageImpl();

        String fullName = pkg.getName() + "." + procName;
        Class<? extends Procedure> procClass = (Class<? extends Procedure>) ClassUtil.getClass(this.classLoader, fullName);

        return new TransactionTypeHistory(procClass, id, false, preExecutionWait, postExecutionWait, isolationLevel);
    }

    public final TransactionType initTransactionType(String procName, int id, long preExecutionWait, long postExecutionWait){
        return initTransactionType(procName, id, preExecutionWait, postExecutionWait, null);
    }

    public final Loader<? extends BenchmarkModule> loadDatabase(int so, Integer isolationLevel) throws SQLException, InterruptedException {
        Loader<? extends BenchmarkModule> loader;

        this.so = so;
        loader = this.makeLoaderImpl();
        this.so = null;

        loadTransactions = new ArrayList<>();
        if (loader != null) {


            try {
                List<LoaderThread> loaderThreads = loader.createLoaderThreads();
                int maxConcurrent = workConf.getLoaderThreads();

                ThreadUtil.runLoaderThreads(loaderThreads, maxConcurrent);

                for(var lt : loaderThreads){
                    if(lt instanceof LoaderThreadHistory){
                        var lth = (LoaderThreadHistory) lt;
                        var events = lth.getEvents();
                        loadTransactions.add(new Transaction(events, 0, so+loadTransactions.size(), IsolationLevel.get(isolationLevel)));
                    }
                }


                if (!loader.getTableCounts().isEmpty()) {
                    LOG.debug("Table Counts:\n{}", loader.getTableCounts());
                }
            } finally {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Finished loading the %s database", this.getBenchmarkName().toUpperCase()));
                }
            }
        }

        return loader;
    }

    public ArrayList<Transaction> getLoadedTransactions() {
        return loadTransactions;
    }

    @Override
    protected final Loader<? extends BenchmarkModule> makeLoaderImpl() {
        return makeLoaderImplHistory(so);
    }

    protected abstract Loader<? extends BenchmarkModule> makeLoaderImplHistory(int so);

}

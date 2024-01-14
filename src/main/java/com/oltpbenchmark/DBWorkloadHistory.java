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


package com.oltpbenchmark;

import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.TransactionTypes;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.apiHistory.BenchmarkModuleHistory;
import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.ResultHistory;
import com.oltpbenchmark.apiHistory.WorkerHistory;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.util.*;
import org.apache.commons.cli.*;
import org.apache.commons.collections4.map.ListOrderedMap;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;

public class DBWorkloadHistory extends DBWorkload{
    private static final Logger LOG = LoggerFactory.getLogger(DBWorkloadHistory.class);

    private static final String SINGLE_LINE = StringUtil.repeat("=", 70);

    private static final String RATE_DISABLED = "disabled";
    private static final String RATE_UNLIMITED = "unlimited";

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        // create the command line parser
        CommandLineParser parser = new DefaultParser();

        XMLConfiguration pluginConfig = buildConfiguration("config/plugin.xml");

        Options options = buildOptions(pluginConfig);

        CommandLine argsLine = parser.parse(options, args);

        if (argsLine.hasOption("h")) {
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("c")) {
            LOG.error("Missing Configuration file");
            printUsage(options);
            return;
        } else if (!argsLine.hasOption("b")) {
            LOG.error("Missing Benchmark Class to load");
            printUsage(options);
            return;
        }


        // Seconds
        int intervalMonitor = 0;
        if (argsLine.hasOption("im")) {
            intervalMonitor = Integer.parseInt(argsLine.getOptionValue("im"));
        }

        // -------------------------------------------------------------------
        // GET PLUGIN LIST
        // -------------------------------------------------------------------

        String targetBenchmarks = argsLine.getOptionValue("b");

        String[] targetList = targetBenchmarks.split(",");
        List<BenchmarkModule> benchList = new ArrayList<>();

        // Use this list for filtering of the output
        List<TransactionType> activeTXTypes = new ArrayList<>();

        String configFile = argsLine.getOptionValue("c");

        XMLConfiguration xmlConfig = buildConfiguration(configFile);

        // Load the configuration for each benchmark
        int lastTxnId = 0;
        for (String plugin : targetList) {
            String pluginTest = "[@bench='" + plugin + "']";

            // ----------------------------------------------------------------
            // BEGIN LOADING WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------

            WorkloadConfiguration wrkld = new WorkloadConfiguration();
            wrkld.setBenchmarkName(plugin);
            wrkld.setXmlConfig(xmlConfig);

            // Pull in database configuration
            wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getString("type")));
            wrkld.setDriverClass(xmlConfig.getString("driver"));
            wrkld.setUrl(xmlConfig.getString("url"));
            wrkld.setUsername(xmlConfig.getString("username"));
            wrkld.setPassword(xmlConfig.getString("password"));
            wrkld.setRandomSeed(xmlConfig.getInt("randomSeed", -1));
            wrkld.setBatchSize(xmlConfig.getInt("batchsize", 128));
            wrkld.setMaxRetries(xmlConfig.getInt("retries", 3));
            wrkld.setNewConnectionPerTxn(xmlConfig.getBoolean("newConnectionPerTxn", false));

            int terminals = xmlConfig.getInt("terminals[not(@bench)]", 0);
            terminals = xmlConfig.getInt("terminals" + pluginTest, terminals);
            wrkld.setTerminals(terminals);

            if (xmlConfig.containsKey("loaderThreads")) {
                int loaderThreads = xmlConfig.getInt("loaderThreads");
                wrkld.setLoaderThreads(loaderThreads);
            }

            String isolationMode = xmlConfig.getString("isolation[not(@bench)]", "TRANSACTION_SERIALIZABLE");
            wrkld.setIsolationMode(xmlConfig.getString("isolation" + pluginTest, isolationMode));
            wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
            wrkld.setDataDir(xmlConfig.getString("datadir", "."));
            wrkld.setDDLPath(xmlConfig.getString("ddlpath", null));

            double selectivity = -1;
            try {
                selectivity = xmlConfig.getDouble("selectivity");
                wrkld.setSelectivity(selectivity);
            } catch (NoSuchElementException nse) {
                // Nothing to do here !
            }

            // ----------------------------------------------------------------
            // CREATE BENCHMARK MODULE
            // ----------------------------------------------------------------

            String classname = pluginConfig.getString("/plugin[@name='" + plugin + "']");

            if (classname == null) {
                throw new ParseException("Plugin " + plugin + " is undefined in config/plugin.xml");
            }

            BenchmarkModule bench = ClassUtil.newInstance(classname, new Object[]{wrkld}, new Class<?>[]{WorkloadConfiguration.class});
            Map<String, Object> initDebug = new ListOrderedMap<>();
            initDebug.put("Benchmark", String.format("%s {%s}", plugin.toUpperCase(), classname));
            initDebug.put("Configuration", configFile);
            initDebug.put("Type", wrkld.getDatabaseType());
            initDebug.put("Driver", wrkld.getDriverClass());
            initDebug.put("URL", wrkld.getUrl());
            initDebug.put("Isolation", wrkld.getIsolationString());
            initDebug.put("Batch Size", wrkld.getBatchSize());
            initDebug.put("Scale Factor", wrkld.getScaleFactor());
            initDebug.put("Terminals", wrkld.getTerminals());
            initDebug.put("New Connection Per Txn", wrkld.getNewConnectionPerTxn());

            if (selectivity != -1) {
                initDebug.put("Selectivity", selectivity);
            }

            LOG.info("{}\n\n{}", SINGLE_LINE, StringUtil.formatMaps(initDebug));
            LOG.info(SINGLE_LINE);

            // ----------------------------------------------------------------
            // LOAD TRANSACTION DESCRIPTIONS
            // ----------------------------------------------------------------
            int numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            if (numTxnTypes == 0 && targetList.length == 1) {
                //if it is a single workload run, <transactiontypes /> w/o attribute is used
                pluginTest = "[not(@bench)]";
                numTxnTypes = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/transactiontype").size();
            }


            List<TransactionType> ttypes = new ArrayList<>();
            ttypes.add(TransactionType.INVALID);
            int txnIdOffset = lastTxnId;
            for (int i = 1; i <= numTxnTypes; i++) {
                String key = "transactiontypes" + pluginTest + "/transactiontype[" + i + "]";
                String txnName = xmlConfig.getString(key + "/name");

                // Get ID if specified; else increment from last one.
                int txnId = i;
                if (xmlConfig.containsKey(key + "/id")) {
                    txnId = xmlConfig.getInt(key + "/id");
                }

                long preExecutionWait = 0;
                if (xmlConfig.containsKey(key + "/preExecutionWait")) {
                    preExecutionWait = xmlConfig.getLong(key + "/preExecutionWait");
                }

                long postExecutionWait = 0;
                if (xmlConfig.containsKey(key + "/postExecutionWait")) {
                    postExecutionWait = xmlConfig.getLong(key + "/postExecutionWait");
                }

                Integer isolation = wrkld.getIsolationMode();
                if(xmlConfig.containsKey(key+ "/isolation")){
                    isolation = IsolationLevel.getMode(xmlConfig.getString(key + "/isolation"));
                }

                TransactionType tmpType = null;

                if(bench instanceof BenchmarkModuleHistory){
                    var benchHist = (BenchmarkModuleHistory) bench;
                    tmpType = benchHist.initTransactionType(txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait, isolation);
                }
                else{
                    tmpType = bench.initTransactionType(txnName, txnId + txnIdOffset, preExecutionWait, postExecutionWait);

                }

                // Keep a reference for filtering
                activeTXTypes.add(tmpType);

                // Add a ref for the active TTypes in this benchmark
                ttypes.add(tmpType);
                lastTxnId = i;
            }

            // Wrap the list of transactions and save them
            TransactionTypes tt = new TransactionTypes(ttypes);
            wrkld.setTransTypes(tt);
            LOG.debug("Using the following transaction types: {}", tt);

            // Read in the groupings of transactions (if any) defined for this
            // benchmark
            int numGroupings = xmlConfig.configurationsAt("transactiontypes" + pluginTest + "/groupings/grouping").size();
            LOG.debug("Num groupings: {}", numGroupings);
            for (int i = 1; i < numGroupings + 1; i++) {
                String key = "transactiontypes" + pluginTest + "/groupings/grouping[" + i + "]";

                // Get the name for the grouping and make sure it's valid.
                String groupingName = xmlConfig.getString(key + "/name").toLowerCase();
                if (!groupingName.matches("^[a-z]\\w*$")) {
                    LOG.error(String.format("Grouping name \"%s\" is invalid." + " Must begin with a letter and contain only" + " alphanumeric characters.", groupingName));
                    System.exit(-1);
                } else if (groupingName.equals("all")) {
                    LOG.error("Grouping name \"all\" is reserved." + " Please pick a different name.");
                    System.exit(-1);
                }

                // Get the weights for this grouping and make sure that there
                // is an appropriate number of them.
                List<String> groupingWeights = Arrays.asList(xmlConfig.getString(key + "/weights").split("\\s*,\\s*"));
                if (groupingWeights.size() != numTxnTypes) {
                    LOG.error(String.format("Grouping \"%s\" has %d weights," + " but there are %d transactions in this" + " benchmark.", groupingName, groupingWeights.size(), numTxnTypes));
                    System.exit(-1);
                }

                LOG.debug("Creating grouping with name, weights: {}, {}", groupingName, groupingWeights);
            }


            benchList.add(bench);

            // ----------------------------------------------------------------
            // WORKLOAD CONFIGURATION
            // ----------------------------------------------------------------

            int size = xmlConfig.configurationsAt("/works/work").size();
            for (int i = 1; i < size + 1; i++) {
                final HierarchicalConfiguration<ImmutableNode> work = xmlConfig.configurationAt("works/work[" + i + "]");
                List<String> weight_strings;

                // use a workaround if there are multiple workloads or single
                // attributed workload
                if (targetList.length > 1 || work.containsKey("weights[@bench]")) {
                    weight_strings = Arrays.asList(work.getString("weights" + pluginTest).split("\\s*,\\s*"));
                } else {
                    weight_strings = Arrays.asList(work.getString("weights[not(@bench)]").split("\\s*,\\s*"));
                }

                double rate = 1;
                boolean rateLimited = true;
                boolean disabled = false;
                boolean timed;

                // can be "disabled", "unlimited" or a number
                String rate_string;
                rate_string = work.getString("rate[not(@bench)]", "");
                rate_string = work.getString("rate" + pluginTest, rate_string);
                if (rate_string.equals(RATE_DISABLED)) {
                    disabled = true;
                } else if (rate_string.equals(RATE_UNLIMITED)) {
                    rateLimited = false;
                } else if (rate_string.isEmpty()) {
                    LOG.error(String.format("Please specify the rate for phase %d and workload %s", i, plugin));
                    System.exit(-1);
                } else {
                    try {
                        rate = Double.parseDouble(rate_string);
                        if (rate <= 0) {
                            LOG.error("Rate limit must be at least 0. Use unlimited or disabled values instead.");
                            System.exit(-1);
                        }
                    } catch (NumberFormatException e) {
                        LOG.error(String.format("Rate string must be '%s', '%s' or a number", RATE_DISABLED, RATE_UNLIMITED));
                        System.exit(-1);
                    }
                }
                Phase.Arrival arrival = Phase.Arrival.REGULAR;
                String arrive = work.getString("@arrival", "regular");
                if (arrive.equalsIgnoreCase("POISSON")) {
                    arrival = Phase.Arrival.POISSON;
                }

                // We now have the option to run all queries exactly once in
                // a serial (rather than random) order.
                boolean serial = Boolean.parseBoolean(work.getString("serial", Boolean.FALSE.toString()));


                int activeTerminals;
                activeTerminals = work.getInt("active_terminals[not(@bench)]", terminals);
                activeTerminals = work.getInt("active_terminals" + pluginTest, activeTerminals);
                // If using serial, we should have only one terminal
                if (serial && activeTerminals != 1) {
                    LOG.warn("Serial ordering is enabled, so # of active terminals is clamped to 1.");
                    activeTerminals = 1;
                }
                if (activeTerminals > terminals) {
                    LOG.error(String.format("Configuration error in work %d: " + "Number of active terminals is bigger than the total number of terminals", i));
                    System.exit(-1);
                }

                int time = work.getInt("/time", 0);
                int warmup = work.getInt("/warmup", 0);
                timed = (time > 0);
                if (!timed) {
                    if (serial) {
                        LOG.info("Timer disabled for serial run; will execute" + " all queries exactly once.");
                    } else {
                        LOG.error("Must provide positive time bound for" + " non-serial executions. Either provide" + " a valid time or enable serial mode.");
                        System.exit(-1);
                    }
                } else if (serial) {
                    LOG.info("Timer enabled for serial run; will run queries" + " serially in a loop until the timer expires.");
                }
                if (warmup < 0) {
                    LOG.error("Must provide non-negative time bound for" + " warmup.");
                    System.exit(-1);
                }

                ArrayList<Double> weights = new ArrayList<>();

                double totalWeight = 0;

                for (String weightString : weight_strings) {
                    double weight = Double.parseDouble(weightString);
                    totalWeight += weight;
                    weights.add(weight);
                }

                long roundedWeight = Math.round(totalWeight);

                if (roundedWeight != 100) {
                    LOG.warn("rounded weight [{}] does not equal 100.  Original weight is [{}]", roundedWeight, totalWeight);
                }


                wrkld.addPhase(i, time, warmup, rate, weights, rateLimited, disabled, serial, timed, activeTerminals, arrival);
            }

            // CHECKING INPUT PHASES
            int j = 0;
            for (Phase p : wrkld.getPhases()) {
                j++;
                if (p.getWeightCount() != numTxnTypes) {
                    LOG.error(String.format("Configuration files is inconsistent, phase %d contains %d weights but you defined %d transaction types", j, p.getWeightCount(), numTxnTypes));
                    if (p.isSerial()) {
                        LOG.error("However, note that since this a serial phase, the weights are irrelevant (but still must be included---sorry).");
                    }
                    System.exit(-1);
                }
            }

            // Generate the dialect map
            wrkld.init();


        }


        // Export StatementDialects
        if (isBooleanOptionSet(argsLine, "dialects-export")) {
            BenchmarkModule bench = benchList.get(0);
            if (bench.getStatementDialects() != null) {
                LOG.info("Exporting StatementDialects for {}", bench);
                String xml = bench.getStatementDialects().export(bench.getWorkloadConfiguration().getDatabaseType(), bench.getProcedures().values());
                LOG.debug(xml);
                System.exit(0);
            }
            throw new RuntimeException("No StatementDialects is available for " + bench);
        }

        // Create the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "create")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                    runCreator(benchmark);
                    LOG.info("Finished creating new {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when creating benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping creating benchmark database tables");
        }

        // Refresh the catalog.
        for (BenchmarkModule benchmark : benchList) {
            benchmark.refreshCatalog();
        }

        // Clear the Benchmark's Database
        if (isBooleanOptionSet(argsLine, "clear")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
                    benchmark.refreshCatalog();
                    benchmark.clearDatabase();
                    benchmark.refreshCatalog();
                    LOG.info("Finished clearing {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when clearing benchmark database tables.", ex);
                System.exit(1);
            }
        } else {
            LOG.debug("Skipping clearing benchmark database tables");
        }


        var transactions = new ArrayList<ArrayList<Transaction>>();
        // Execute Loader
        ArrayList<Transaction> initTransactions = new ArrayList<>();
        //Dummy transaction containing (after execution) all rows not loaded.
        initTransactions.add(new Transaction(new ArrayList<>(), 0, 0, IsolationLevel.get(Connection.TRANSACTION_SERIALIZABLE)));

        if (isBooleanOptionSet(argsLine, "load")) {
            try {
                for (BenchmarkModule benchmark : benchList) {
                    LOG.info("Loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
                    var wrkld = benchmark.getWorkloadConfiguration();
                    var t = runLoader(benchmark, initTransactions.size(), wrkld.getIsolationMode());
                    if(t != null){
                        initTransactions.addAll(t);
                    }
                    LOG.info("Finished loading data into {} database...", benchmark.getBenchmarkName().toUpperCase());
                }
            } catch (Throwable ex) {
                LOG.error("Unexpected error when loading benchmark database records.", ex);
                System.exit(1);
            }

        } else {
            LOG.debug("Skipping loading benchmark database records");
        }

        transactions.add(initTransactions);
        // Execute Workload
        if (isBooleanOptionSet(argsLine, "execute")) {
            // Bombs away!
            try {
                Results r = runWorkload(benchList, intervalMonitor, transactions);
                writeOutputs(r, activeTXTypes, argsLine, xmlConfig);
                writeHistograms(r);

                if (argsLine.hasOption("json-histograms")) {
                    String histogram_json = writeJSONHistograms(r);
                    String fileName = argsLine.getOptionValue("json-histograms");
                    FileUtil.writeStringToFile(new File(fileName), histogram_json);
                    LOG.info("Histograms JSON Data: " + fileName);
                }
                /*
                if(r.getRetry().getSampleCount() == 0 && r.getError().getSampleCount() == 0 && r.getRetryDifferent().getSampleCount() == 0 && r.getUnknown().getSampleCount() == 0) {

                }
                */

                var results = evaluateHistory(transactions);
                writeOutputHistory(results, argsLine);

            } catch (Throwable ex) {
                LOG.error("Unexpected error when executing benchmarks.", ex);
                System.exit(1);
            }

        } else {
            LOG.info("Skipping benchmark workload execution");
        }
    }

    private static ArrayList<Transaction> runLoader(BenchmarkModule bench, int so, Integer isolationLevel) throws SQLException, InterruptedException {
        LOG.debug(String.format("Loading %s Database", bench));

        if(bench instanceof BenchmarkModuleHistory){
            var bh = (BenchmarkModuleHistory) bench;
            bh.loadDatabase(so, isolationLevel);
            return bh.getLoadedTransactions();
        }
        else{
            bench.loadDatabase();
            return null;
        }
    }

    private static Results runWorkload(List<BenchmarkModule> benchList, int intervalMonitor, ArrayList<ArrayList<Transaction>> transactions) throws IOException {
        List<Worker<?>> workers = new ArrayList<>();
        List<WorkloadConfiguration> workConfs = new ArrayList<>();
        for (BenchmarkModule bench : benchList) {
            LOG.info("Creating {} virtual terminals...", bench.getWorkloadConfiguration().getTerminals());
            workers.addAll(bench.makeWorkers());

            int num_phases = bench.getWorkloadConfiguration().getNumberOfPhases();
            LOG.info(String.format("Launching the %s Benchmark with %s Phase%s...", bench.getBenchmarkName().toUpperCase(), num_phases, (num_phases > 1 ? "s" : "")));
            workConfs.add(bench.getWorkloadConfiguration());

        }
        Results r = ThreadBench.runRateLimitedBenchmark(workers, workConfs, intervalMonitor);
        LOG.info(SINGLE_LINE);
        LOG.info("Rate limited reqs/s: {}", r);

        for(var w: workers){
            if(w instanceof WorkerHistory<?> wh){
                transactions.add(wh.getTransactions());
            }
        }

        return r;
    }

    private static ResultHistory evaluateHistory(ArrayList<ArrayList<Transaction>> transactions){


        var r = new ResultHistory();
        r.setTimeout(false);


        ExecutorService executor = Executors.newCachedThreadPool();
        Callable<Object> task = () -> {
            LOG.info("Creating history...");

            r.setCreateStartTime(System.nanoTime());
            History h = new History(transactions);
            r.setCreateEndTime(System.nanoTime());

            LOG.info("Evaluating history...");

            r.setEvalStartTime(System.nanoTime());
            var ret = h.checkSOBound(r);
            r.setConsistent(ret);
            r.setEvalEndTime(System.nanoTime());
            return ret;
        };
        Future<Object> future = executor.submit(task);
        try {
            //Object result = future.get(Integer.MAX_VALUE, TimeUnit.MINUTES);
            Object result = future.get(1, TimeUnit.MINUTES);
        } catch (TimeoutException ex) {
            r.setTimeout(true);
            // handle the timeout
        } catch (InterruptedException e) {
            r.setTimeout(true);
            // handle the interrupts
        } catch (ExecutionException e) {
            // handle other exceptions
        } finally {
            if(r.getCreateStartTime() == null){
                r.setCreateStartTime(System.nanoTime());
            }
            if(r.getCreateEndTime() == null){
                r.setCreateEndTime(System.nanoTime());
            }
            if(r.getEvalStartTime() == null){
                r.setEvalStartTime(System.nanoTime());
            }
            if(r.getEvalEndTime() == null){
                r.setEvalEndTime(System.nanoTime());
            }
            future.cancel(true); // may or may not desire this
            executor.shutdown();
        }

        if(r.getConsistent() == null){
            LOG.info("The procedure timeout!");
        }
        else if(r.getConsistent()){
            LOG.info("The history is consistent");
        }
        else{
            LOG.info("The history is NOT consistent!");
        }
        return r;

    }


    private static void writeOutputHistory(ResultHistory r, CommandLine argsLine) throws Exception {

        // If an output directory is used, store the information
        String outputDirectory = "results";

        if (argsLine.hasOption("d")) {
            outputDirectory = argsLine.getOptionValue("d");
        }

        FileUtil.makeDirIfNotExists(outputDirectory);

        String name = StringUtils.join(StringUtils.split(argsLine.getOptionValue("b"), ','), '-');

        String baseFileName = name + "_" + TimeUtil.getCurrentTimeString();

        String summaryFileName = baseFileName + ".histories.csv";

        var timeC = r.getCreateEndTime() - r.getCreateStartTime();
        var timeE = r.getEvalEndTime() - r.getEvalStartTime();

        double timeCMS = ((double) timeC / (double) 1000000);
        double timeEMS = ((double) timeE / (double) 1000000);

        try (PrintStream ps = new PrintStream(FileUtil.joinPath(outputDirectory, summaryFileName))) {
            LOG.info("Output history data into file: {}", summaryFileName);

            var cons = r.getConsistent() == null ? "Unknown" : r.getConsistent();

            ps.println("Creation Time (ms), Evaluation Time (ms), Time (ms), Timeout, Consistent");
            ps.println(timeCMS + ", " + timeEMS + ", " + (timeCMS + timeEMS) + ", " + r.isTimeout() + ", " + cons);
        }

    }

}

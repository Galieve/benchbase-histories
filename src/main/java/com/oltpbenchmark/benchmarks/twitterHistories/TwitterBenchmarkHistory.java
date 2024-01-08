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


package com.oltpbenchmark.benchmarks.twitterHistories;

import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.apiHistory.BenchmarkModuleHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.procedures.GetFollowersHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.util.TraceTransactionGeneratorHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.util.TwitterOperationHistory;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class TwitterBenchmarkHistory extends BenchmarkModuleHistory {

    private final TwitterConfigurationHistory twitterConf;

    public TwitterBenchmarkHistory(WorkloadConfiguration workConf) {
        super(workConf);
        this.twitterConf = new TwitterConfigurationHistory(workConf);
    }

    @Override
    protected Package getProcedurePackageImpl() {
        return GetFollowersHistory.class.getPackage();
    }

    @Override
    protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl() throws IOException {
        List<String> tweetIds = FileUtils.readLines(new File(twitterConf.getTracefile()), Charset.defaultCharset());
        List<String> userIds = FileUtils.readLines(new File(twitterConf.getTracefile2()), Charset.defaultCharset());

        if (tweetIds.size() != userIds.size()) {
            throw new RuntimeException(String.format("there was a problem reading files, sizes don't match.  tweets %d, userids %d", tweetIds.size(), userIds.size()));
        }

        List<TwitterOperationHistory> trace = new ArrayList<>();
        for (int i = 0; i < tweetIds.size(); i++) {
            trace.add(new TwitterOperationHistory(Integer.parseInt(tweetIds.get(i)), Integer.parseInt(userIds.get(i))));
        }

        List<Worker<? extends BenchmarkModule>> workers = new ArrayList<>();
        for (int i = 1; i <= workConf.getTerminals(); ++i) {
            TransactionGenerator<TwitterOperationHistory> generator = new TraceTransactionGeneratorHistory(trace);
            workers.add(new TwitterWorkerHistory(this, i, generator));
        }
        return workers;
    }

    @Override
    protected Loader<? extends BenchmarkModule> makeLoaderImplHistory(int so) {
        return new TwitterLoaderHistory(this, so);
    }
}

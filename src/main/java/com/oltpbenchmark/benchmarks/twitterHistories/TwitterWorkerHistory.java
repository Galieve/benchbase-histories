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

import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.api.TransactionGenerator;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.apiHistory.WorkerHistory;
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.benchmarks.twitterHistories.procedures.*;
import com.oltpbenchmark.benchmarks.twitterHistories.util.TweetHistogramHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.util.TwitterOperationHistory;
import com.oltpbenchmark.types.TransactionStatus;
import com.oltpbenchmark.util.RandomDistribution.FlatHistogram;
import com.oltpbenchmark.util.TextGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;

public class TwitterWorkerHistory extends WorkerHistory<TwitterBenchmarkHistory> {
    private final TransactionGenerator<TwitterOperationHistory> generator;

    private final FlatHistogram<Integer> tweet_len_rng;
    private final int num_users;

    public TwitterWorkerHistory(TwitterBenchmarkHistory benchmarkModule, int id, TransactionGenerator<TwitterOperationHistory> generator) {
        super(benchmarkModule, id);
        this.generator = generator;
        this.num_users = (int) Math.round(TwitterConstantsHistory.NUM_USERS * this.getWorkloadConfiguration().getScaleFactor());

        TweetHistogramHistory tweet_h = new TweetHistogramHistory();
        this.tweet_len_rng = new FlatHistogram<>(this.rng(), tweet_h);
    }

    @Override
    protected TransactionStatus executeWorkHistory(Connection conn, TransactionType nextTrans, ArrayList<Event> events, int id, int soID) throws UserAbortException, SQLException {
        TwitterOperationHistory t = generator.nextTransaction();
        // zero is an invalid id, so fixing random here to be atleast 1
        t.uid = this.rng().nextInt(this.num_users - 1 ) + 1;

        if (nextTrans.getProcedureClass().equals(GetTweetHistory.class)) {
            doSelect1Tweet(conn, t.tweetid, events, id, soID);
        } else if (nextTrans.getProcedureClass().equals(GetTweetsFromFollowingHistory.class)) {
            doSelectTweetsFromPplIFollow(conn, t.uid, events, id, soID);
        } else if (nextTrans.getProcedureClass().equals(GetFollowersHistory.class)) {
            doSelectNamesOfPplThatFollowMe(conn, t.uid, events, id, soID);
        } else if (nextTrans.getProcedureClass().equals(GetUserTweetsHistory.class)) {
            doSelectTweetsForUid(conn, t.uid, events, id, soID);
        } else if (nextTrans.getProcedureClass().equals(InsertTweetHistory.class)) {
            int len = this.tweet_len_rng.nextValue();
            String text = TextGenerator.randomStr(this.rng(), len);
            doInsertTweet(conn, t.uid, text, events, id, soID);
        }
        return (TransactionStatus.SUCCESS);
    }

    public void doSelect1Tweet(Connection conn, int tweet_id, ArrayList<Event> events, int id, int soID) throws SQLException {
        GetTweetHistory proc = this.getProcedure(GetTweetHistory.class);

        proc.run(conn, tweet_id, events, id, soID);
    }

    public void doSelectTweetsFromPplIFollow(Connection conn, int uid, ArrayList<Event> events, int id, int soID) throws SQLException {
        GetTweetsFromFollowingHistory proc = this.getProcedure(GetTweetsFromFollowingHistory.class);

        proc.run(conn, uid, events, id, soID);
    }

    public void doSelectNamesOfPplThatFollowMe(Connection conn, int uid, ArrayList<Event> events, int id, int soID) throws SQLException {
        GetFollowersHistory proc = this.getProcedure(GetFollowersHistory.class);

        proc.run(conn, uid, events, id, soID);
    }

    public void doSelectTweetsForUid(Connection conn, int uid, ArrayList<Event> events, int id, int soID) throws SQLException {
        GetUserTweetsHistory proc = this.getProcedure(GetUserTweetsHistory.class);

        proc.run(conn, uid, events, id, soID);
    }

    public void doInsertTweet(Connection conn, int uid, String text, ArrayList<Event> events, int id, int soID) throws SQLException {
        InsertTweetHistory proc = this.getProcedure(InsertTweetHistory.class);

        Time time = new Time(System.currentTimeMillis());
        proc.run(conn, uid, text, time, events, id, soID);

    }

}
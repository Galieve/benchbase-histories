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


package com.oltpbenchmark.benchmarksHistory.twitterHistories.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarksHistory.twitterHistories.TwitterConstantsHistory;
import com.oltpbenchmark.benchmarksHistory.twitterHistories.pojo.Follow;
import com.oltpbenchmark.benchmarksHistory.twitterHistories.pojo.Tweet;
import com.oltpbenchmark.historyModelHistory.ProcedureHistory;
import com.oltpbenchmark.historyModelHistory.events.Event;
import com.oltpbenchmark.historyModelHistory.events.SelectEvent;
import com.oltpbenchmark.historyModelHistory.events.Value;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.function.Function;

public class GetTweetsFromFollowingHistory extends ProcedureHistory {

    public final SQLStmt getFollowing = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_FOLLOWS + " WHERE f1 = ?");
    //LIMIT " + TwitterConstantsHistory.LIMIT_FOLLOWERS

    /**
     * NOTE: The ?? is substituted into a string of repeated ?'s
     */
    public final SQLStmt getTweets = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_TWEETS + " WHERE uid IN (??)", TwitterConstantsHistory.LIMIT_FOLLOWERS);


    public void run(Connection conn, int uid, ArrayList<Event> events, int id, int so) throws SQLException {
        try (PreparedStatement getFollowingStatement = this.getPreparedStatement(conn, getFollowing)) {
            getFollowingStatement.setLong(1, uid);
            try (ResultSet followingResult = getFollowingStatement.executeQuery()) {

                var f = new Follow();
                var info = f.getSelectEventInfo(followingResult);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    Long.parseLong(val.getValue("F1")) == uid;

                events.add(new SelectEvent(id, so, 0, info, where, f.getTableNames()));

                try (PreparedStatement stmt = this.getPreparedStatement(conn, getTweets)) {
                    int ctr = 0;
                    long last = -1;
                    var uids = new HashSet<Long>();
                    followingResult.beforeFirst();
                    while (followingResult.next() && ctr++ < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                        last = followingResult.getLong(1);
                        stmt.setLong(ctr, last);
                        uids.add(last);

                    }
                    if (ctr > 0) {
                        while (ctr++ < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                            stmt.setLong(ctr, last);
                            uids.add(last);
                        }
                        try (ResultSet getTweetsResult = stmt.executeQuery()) {
                            Function<Value, Boolean> whereTweets = (val) ->
                                val != null &&
                                uids.contains(Long.parseLong(val.getValue("UID")));
                            var t = new Tweet();
                            var wro = t.getSelectEventInfo(getTweetsResult);
                            events.add(new SelectEvent(id, so, 1, wro, whereTweets, t.getTableNames()));
                        }
                    } else {
                        // LOG.debug("No followers for user: "+uid); // so what .. ?
                    }
                }
            }
        }
    }

}

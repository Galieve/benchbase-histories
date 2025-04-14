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


package com.oltpbenchmark.benchmarksHistory.twitterFull.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.historyModelHistory.ProcedureHistory;
import com.oltpbenchmark.historyModelHistory.events.Event;
import com.oltpbenchmark.historyModelHistory.events.SelectEvent;
import com.oltpbenchmark.historyModelHistory.events.Value;
import com.oltpbenchmark.benchmarksHistory.twitterFull.TwitterConstantsHistory;
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.Follow;
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.Tweet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class GetTweetsFromFollowingFull extends ProcedureHistory {

    public final SQLStmt getFollowing = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_FOLLOWS + " WHERE f1 = ? AND f2 = ?");
    //LIMIT " + TwitterConstantsHistory.LIMIT_FOLLOWERS

    /**
     * NOTE: The ?? is substituted into a string of repeated ?'s
     */
    public final SQLStmt getTweets = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_TWEETS + " WHERE id = ?");


    public void run(Connection conn, int numUsers, int numTweets, int uid, ArrayList<Event> events, int id, int so) throws SQLException {

        int po = 0;
        try (PreparedStatement getFollowingStatement = this.getPreparedStatement(conn, getFollowing)) {
            getFollowingStatement.setLong(1, uid);
            var following = new ArrayList<Long>();

            for(int i = 0; i < numUsers; ++i) {
                getFollowingStatement.setLong(2, i);
                try (ResultSet followingResult = getFollowingStatement.executeQuery()) {

                    var f = new Follow();
                    var info = f.getSelectEventInfo(followingResult);
                    int finalI = i;
                    Function<Value, Boolean> where = (val) ->
                        val != null &&
                        Long.parseLong(val.getValue("F1")) == uid &&
                        Long.parseLong(val.getValue("F2")) == finalI;

                    events.add(new SelectEvent(id, so, po, info, where, f.getTableNames()));
                    ++po;

                    if (followingResult.next() && following.size() < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                        following.add(followingResult.getLong("F2"));
                    }
                }
            }

            try (PreparedStatement stmt = this.getPreparedStatement(conn, getTweets)) {
                int ctr = 0;
                long last = -1;
                for(int i = 1; i <= numTweets; ++i){

                    last = following.get(ctr);
                    stmt.setLong(1, last);
                    try (ResultSet getTweetsResult = stmt.executeQuery()) {
                        int finalI = i;
                        Function<Value, Boolean> whereTweets = (val) ->
                            val != null &&
                            Long.parseLong(val.getValue("ID")) == finalI;
                        var t = new Tweet();
                        var wro = t.getSelectEventInfo(getTweetsResult);
                        events.add(new SelectEvent(id, so, po, wro, whereTweets, t.getTableNames()));
                        ++po;
                        //If the tweet belongs to the user or not we do not care, as here is the end of the code.
                    }

                }
                /*
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
           */
            }

        }

    }

}

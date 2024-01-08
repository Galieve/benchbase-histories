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

package com.oltpbenchmark.benchmarks.twitterHistories.procedures;

import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.apiHistory.ProcedureHistory;
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.SelectEvent;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.benchmarks.twitterHistories.TwitterConstantsHistory;
import com.oltpbenchmark.benchmarks.twitterHistories.pojo.Follower;
import com.oltpbenchmark.benchmarks.twitterHistories.pojo.User;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.function.Function;

public class GetFollowersHistory extends ProcedureHistory {

    public final SQLStmt getFollowers = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_FOLLOWERS + " WHERE f1 = ?");
    //LIMIT " + TwitterConstantsHistory.LIMIT_FOLLOWERS

    /**
     * NOTE: The ?? is substituted into a string of repeated ?'s
     */
    public final SQLStmt getFollowerNames = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_USER + " WHERE uid IN (??)", TwitterConstantsHistory.LIMIT_FOLLOWERS);

    public void run(Connection conn, long uid, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getFollowers)) {
            stmt.setLong(1, uid);
            try (ResultSet rs = stmt.executeQuery()) {

                var f = new Follower();
                var info = f.getSelectEventInfo(rs);
                Function<Value, Boolean> where = (val) ->
                    val != null &&
                    Long.parseLong(val.getValue("F1")) == uid;

                events.add(new SelectEvent(id, soID, po, info, where, f.getTableNames()));
                ++po;

                try (PreparedStatement getFollowerNamesstmt = this.getPreparedStatement(conn, getFollowerNames)) {
                    int ctr = 0;
                    long last = -1;

                    var followers = new HashSet<Long>();

                    rs.beforeFirst();
                    while (rs.next() && ctr++ < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                        last = rs.getLong(1);
                        getFollowerNamesstmt.setLong(ctr, last);
                        followers.add(last);
                    }
                    if (ctr > 0) {
                        while (ctr++ < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                            getFollowerNamesstmt.setLong(ctr, last);
                            followers.add(last);
                        }
                        try (ResultSet getFollowerNamesrs = getFollowerNamesstmt.executeQuery()) {

                            var u = new User();
                            var wro = u.getSelectEventInfo(getFollowerNamesrs);
                            Function<Value, Boolean> whereFollowers = (val) ->
                                val != null &&
                                followers.contains(Long.parseLong(val.getValue("UID")));
                            events.add(new SelectEvent(id, soID, po, wro, whereFollowers, u.getTableNames()));

                        }
                    }
                }
            }
        }
        // LOG.warn("No followers for user : "+uid); //... so what ?
    }

}

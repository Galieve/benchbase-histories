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
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.Follower;
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.User;
import com.oltpbenchmark.historyModelHistory.ProcedureHistory;
import com.oltpbenchmark.historyModelHistory.events.Event;
import com.oltpbenchmark.historyModelHistory.events.SelectEvent;
import com.oltpbenchmark.historyModelHistory.events.Value;
import com.oltpbenchmark.benchmarksHistory.twitterFull.TwitterConstantsHistory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class GetFollowersFull extends ProcedureHistory {

    public final SQLStmt getFollowers = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_FOLLOWERS + " WHERE f1 = ? AND f2 = ?");

    public final SQLStmt getFollowerName = new SQLStmt("SELECT * FROM " + TwitterConstantsHistory.TABLENAME_USER + " WHERE uid = ?");

    public void run(Connection conn, int numUsers, long uid, ArrayList<Event> events, int id, int soID) throws SQLException {

        int po = 0;

        try (PreparedStatement stmt = this.getPreparedStatement(conn, getFollowers)) {
            var followers = new ArrayList<Long>();
            stmt.setLong(1, uid);

            for(int i = 0; i < numUsers; ++i){
                stmt.setLong(2, i);
                try (ResultSet rs = stmt.executeQuery()) {

                    var f = new Follower();
                    var info = f.getSelectEventInfo(rs);
                    int finalI = i;
                    Function<Value, Boolean> where = (val) ->
                        val != null &&
                        Long.parseLong(val.getValue("F1")) == uid &&
                        Long.parseLong(val.getValue("F2")) == finalI;

                    events.add(new SelectEvent(id, soID, po, info, where, f.getTableNames()));
                    ++po;

                    if(rs.next()){
                        followers.add(rs.getLong("F2"));
                    }

                }
            }
            try (PreparedStatement getFollowerNamestmt = this.getPreparedStatement(conn, getFollowerName)) {
                    int ctr = 0;
                    long last = -1;

                    while (ctr < followers.size() && ctr < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                        last = followers.get(ctr);
                        getFollowerNamestmt.setLong(1, last);

                        try (ResultSet getFollowerNamers = getFollowerNamestmt.executeQuery()) {
                            var u = new User();
                            var wro = u.getSelectEventInfo(getFollowerNamers);
                            long finalLast = last;
                            Function<Value, Boolean> whereFollowers = (val) ->
                                val != null &&
                                Long.parseLong(val.getValue("UID")) == finalLast;
                            events.add(new SelectEvent(id, soID, po, wro, whereFollowers, u.getTableNames()));
                            ++po;
                        }
                        ++ctr;
                    }
                    /*
                    if (ctr > 0) {

                        while (ctr++ < TwitterConstantsHistory.LIMIT_FOLLOWERS) {
                            getFollowerNamestmt.setLong(ctr, last);
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
                    */
            }
        }
        // LOG.warn("No followers for user : "+uid); //... so what ?
    }

}

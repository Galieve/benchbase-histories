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
import com.oltpbenchmark.historyModelHistory.events.EventID;
import com.oltpbenchmark.historyModelHistory.events.InsertEvent;
import com.oltpbenchmark.benchmarksHistory.twitterFull.TwitterConstantsHistory;
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.AddedTweet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;

public class InsertTweetFull extends ProcedureHistory {

    //FIXME: Carlo is this correct? 1) added_tweets is empty initially 2) id is supposed to be not null
    public final SQLStmt insertTweet = new SQLStmt("INSERT INTO " + TwitterConstantsHistory.TABLENAME_ADDED_TWEETS + " (uid,text,createdate, writeID) VALUES (?, ?, ?, ?) RETURNING *");

    public boolean run(Connection conn, long uid, String text, Time time, ArrayList<Event> events, int id, int so) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, insertTweet)) {

            var writeID = EventID.generateID(id, so, 0);
            stmt.setLong(1, uid);
            stmt.setString(2, text);
            stmt.setDate(3, new java.sql.Date(System.currentTimeMillis()));
            stmt.setString(4, writeID);

            var ret = (stmt.execute());
            var rs = stmt.getResultSet();
            var at = new AddedTweet();
            var info = at.getInsertEventInfo(rs);
            events.add(new InsertEvent(id, so, 0, info, at.getTableNames()));
            return ret;
        }
    }
}

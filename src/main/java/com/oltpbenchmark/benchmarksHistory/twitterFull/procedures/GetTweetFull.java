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
import com.oltpbenchmark.benchmarksHistory.twitterFull.pojo.Tweet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.function.Function;

public class GetTweetFull extends ProcedureHistory {

    public SQLStmt getTweet = new SQLStmt(
        "SELECT * FROM " + TwitterConstantsHistory.TABLENAME_TWEETS + " WHERE id = ?"
    );

    public void run(Connection conn, long tweet_id, ArrayList<Event> events, int id, int so) throws SQLException {
        try (PreparedStatement stmt = this.getPreparedStatement(conn, getTweet)) {
            stmt.setLong(1, tweet_id);
            try (ResultSet rs = stmt.executeQuery()) {
                Function<Value, Boolean> where = (val)-> (
                    val != null &&
                    Long.parseLong(val.getValue("ID")) == tweet_id);
                var t = new Tweet();
                var wro = t.getSelectEventInfo(rs);
                events.add(new SelectEvent(id, so, 0, wro, where, t.getTableNames()));
            }
        }
    }
}

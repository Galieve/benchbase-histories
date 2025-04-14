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

package com.oltpbenchmark.historyModelHistory;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.jdbc.AutoIncrementPreparedStatement;
import com.oltpbenchmark.types.DatabaseType;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public abstract class ProcedureHistory extends Procedure {

    /**
     * Constructor
     */
    protected ProcedureHistory() {
        super();
    }



    /**
     * Return a PreparedStatement for the given SQLStmt handle
     * The underlying Procedure API will make sure that the proper SQL
     * for the target DBMS is used for this SQLStmt.
     *
     * @param conn
     * @param stmt
     * @param is
     * @return
     * @throws SQLException
     */
    public final PreparedStatement getPreparedStatementReturnKeys(Connection conn, SQLStmt stmt, int[] is) throws SQLException {
        PreparedStatement pStmt = null;

        // HACK: If the target system is Postgres, wrap the PreparedStatement in a special
        //       one that fakes the getGeneratedKeys().
        if (is != null && (
            this.dbType == DatabaseType.POSTGRES
            || this.dbType == DatabaseType.COCKROACHDB
            || this.dbType == DatabaseType.SQLSERVER
            || this.dbType == DatabaseType.SQLAZURE
        )
        ) {
            pStmt = new AutoIncrementPreparedStatement(this.dbType, conn.prepareStatement(stmt.getSQL(), ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));
        }
        // Everyone else can use the regular getGeneratedKeys() method
        else if (is != null) {
            pStmt = conn.prepareStatement(stmt.getSQL(), is);
        }
        // They don't care about keys
        else {
            pStmt = conn.prepareStatement(stmt.getSQL(), ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
        }

        return (pStmt);
    }

}

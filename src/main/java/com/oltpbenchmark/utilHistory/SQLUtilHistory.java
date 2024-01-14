package com.oltpbenchmark.utilHistory;

import com.oltpbenchmark.util.SQLUtil;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class SQLUtilHistory extends SQLUtil {

    public static List<Object[]> toList(ResultSet rs, Set<String> columnNames) throws SQLException {
        ResultSetMetaData rs_md = rs.getMetaData();
        int num_cols = rs_md.getColumnCount();

        List<Object[]> results = new ArrayList<>();
        while (rs.next()) {
            Object[] row = new Object[num_cols];
            for (int i = 0; i < num_cols; i++) {
                if(columnNames.contains(rs_md.getColumnName(i)))
                    row[i] = rs.getObject(i + 1);
            }
            results.add(row);
        }

        return results;
    }

    public static int size(ResultSet rs) throws SQLException {
        int now = rs.getRow();
        rs.last();
        int numRows = rs.getRow();
        rs.absolute(now);
        return numRows;
    }

}

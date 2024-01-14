package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.apiHistory.events.EventID;
import com.oltpbenchmark.apiHistory.events.Value;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.util.Pair;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

public interface PojoHistory {
    List<Pair<String, String>> getPKsList();
    List<Pair<String, String>> getValuesList();

    private String getWriteID(ResultSet rs, int update) throws SQLException {
        return rs.getString("WRITEID").split(";")[update];
    }

    private String getValue(String type, String field, ResultSet rs, int update) throws SQLException {
        return switch (type) {
            case "INT" -> String.valueOf(rs.getInt(field));
            case "LONG" -> String.valueOf(rs.getLong(field));
            case "STRING" -> field.equals("WRITEID") ? getWriteID(rs, update) : rs.getString(field) == null ? null : rs.getString(field);
            case "FLOAT" -> rs.getBigDecimal(field) == null ? null : rs.getBigDecimal(field).toString();
            case "TIMESTAMP" -> rs.getTimestamp(field) == null ? null :rs.getTimestamp(field).toString();
            case "BOOLEAN" -> String.valueOf(rs.getBoolean(field));
            default -> null;
        };
    }
    public default HashMap<Variable, EventID>
    getSelectEventInfo(ResultSet rs) throws SQLException{
        return getEventInfo((val)-> val, rs, 0).second;
    }

    public default HashMap<Variable, Value>
    getInsertEventInfo(ResultSet rs) throws SQLException{
        return getEventInfo((val)-> val, rs, 0).first;
    }

    public default Pair<HashMap<Variable, Value>, HashMap<Variable, EventID>>
    getDeleteEventInfo(ResultSet rs) throws SQLException{
        return getEventInfo((val)-> null, rs, 0);
    }

    public default Pair<HashMap<Variable, Value>, HashMap<Variable, EventID>> getUpdateEventInfo(ResultSet rs) throws SQLException{
        return getEventInfo((val)->val, rs, 1);
    }

    private Pair<HashMap<Variable, Value>, HashMap<Variable, EventID>> getEventInfo(Function<Value, Value> set, ResultSet rs, int update) throws SQLException {
        HashMap<Variable, Value> writtenVariables = new HashMap<>();
        HashMap<Variable, EventID> wroInverse = new HashMap<>();

        int now = rs.getRow();

        var fieldsPK = getPKsList();
        var fieldsVal = getValuesList();

        rs.beforeFirst();
        while(rs.next()){
            ArrayList<String> vPK = new ArrayList<>();
            HashMap<String, String> vValues = new HashMap<>();


            for(var pairFields : fieldsPK){
                var type = pairFields.first;
                var field = pairFields.second;
                vPK.add(getValue(type, field, rs, update));
            }

            for(var pairFields : fieldsVal){
                var type = pairFields.first;
                var field = pairFields.second;
                vValues.put(field, getValue(type, field, rs, update));
            }

            var variable = new Variable(this.getClass().getName() + ", " + String.join(", ", vPK));
            var value = new Value(variable, vValues);
            writtenVariables.put(variable, set.apply(value));
            wroInverse.put(variable, new EventID(getWriteID(rs, update)));
        }

        rs.absolute(now);
        return new Pair<>(writtenVariables, wroInverse);
    }

    public Set<String> getTableNames();


}

package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.Set;

public class InsertEvent extends Event implements WriteEvent{

    protected HashMap<Variable, Value> writeSet;
    public InsertEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet, Set<String> tableNames){
        super(EventType.INSERT, id, so, po, tableNames);
        this.writeSet = writeSet;
    }

    @Override
    public boolean writes(Variable variable) {
        return writeSet.containsKey(variable);
    }
    @Override
    public Value getWrittenValue(Variable variable) {
        return writeSet.get(variable);
    }

    @Override
    public boolean isRead() {
        return false;
    }

    @Override
    public HashMap<Variable, Value> getWriteSet() {
        return writeSet;
    }
}

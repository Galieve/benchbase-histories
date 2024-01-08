package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class InitialAbsentEvent extends Event implements WriteEvent {

    protected HashMap<Variable, Value> writeSet;

    public InitialAbsentEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet) {
        super(EventType.INITIALABSENT, id, so, po, new HashSet<>());
        this.writeSet = writeSet;
    }

    @Override
    public boolean writes(Variable variable) {
        return writeSet.containsKey(variable);
    }

    @Override
    //If isWritten(v) == true but getWrittenValue(v) == null, the variable is deleted.
    public Value getWrittenValue(Variable variable) {
        return null;
    }

    @Override
    public HashMap<Variable, Value> getWriteSet() {
        return writeSet;
    }

    @Override
    public boolean isRead() {
        return false;
    }

    @Override
    public boolean belongsInTable(Variable variable) {
        return true;
    }
}

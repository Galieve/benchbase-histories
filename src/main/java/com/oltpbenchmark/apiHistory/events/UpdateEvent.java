package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

public class UpdateEvent extends Event{

    public UpdateEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate, Set<String> tableNames) {
        super(EventType.UPDATE, id, so, po, tableNames);
        this.re = new ReadEvent(this, wroInverse, wherePredicate);
        this.we = new WriteEvent(this, writeSet);
    }

    @Override
    public boolean isRead() {
        return true;
    }

    @Override
    public boolean isWrite() {
        return true;
    }
}

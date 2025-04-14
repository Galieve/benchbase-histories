package com.oltpbenchmark.historyModelHistory.events;

import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

public class DeleteEvent extends Event{


    public DeleteEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate, Set<String> tableNames) {
        super(EventType.DELETE, id, so, po, tableNames);
        this.re = new ReadEvent(this, wroInverse, wherePredicate);
        this.we = new WriteEvent(this, writeSet);
    }

    protected DeleteEvent(DeleteEvent e){
        super(e);
    }

    @Override
    public boolean isRead() {
        return true;
    }

    @Override
    public boolean isWrite() {
        return true;
    }

    @Override
    public Event cloneEvent() {
        return new DeleteEvent(this);
    }
}

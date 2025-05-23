package com.oltpbenchmark.historyModelHistory.events;

import java.util.HashMap;
import java.util.Set;

public class InsertEvent extends Event {

    public InsertEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet, Set<String> tableNames){
        super(EventType.INSERT, id, so, po, tableNames);
        this.we = new WriteEvent(this, writeSet);
    }

    protected InsertEvent(InsertEvent e){
        super(e);
    }

    @Override
    public boolean isRead() {
        return false;
    }

    @Override
    public boolean isWrite() {
        return true;
    }

    @Override
    public Event cloneEvent() {
        return new InsertEvent(this);
    }
}

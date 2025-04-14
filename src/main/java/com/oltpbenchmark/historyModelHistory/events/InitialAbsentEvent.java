package com.oltpbenchmark.historyModelHistory.events;

import java.util.HashMap;
import java.util.HashSet;

public class InitialAbsentEvent extends Event{


    public InitialAbsentEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet) {
        super(EventType.INITIALABSENT, id, so, po, new HashSet<>());
        this.we = new WriteEvent(this, writeSet);
    }

    protected InitialAbsentEvent(InitialAbsentEvent e) {
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
        return new InitialAbsentEvent(this);
    }
}

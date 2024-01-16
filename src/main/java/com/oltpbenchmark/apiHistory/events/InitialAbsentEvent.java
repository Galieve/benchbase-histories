package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

public class InitialAbsentEvent extends Event{


    public InitialAbsentEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet) {
        super(EventType.INITIALABSENT, id, so, po, new HashSet<>());
        this.we = new WriteEvent(this, writeSet);
    }


    @Override
    public boolean isRead() {
        return false;
    }

    @Override
    public boolean isWrite() {
        return true;
    }

}

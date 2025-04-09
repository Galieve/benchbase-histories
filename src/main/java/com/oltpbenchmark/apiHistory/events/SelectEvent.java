package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

public class SelectEvent extends Event {

    public SelectEvent(Integer id, Integer so, Integer po, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate, Set<String> tableNames) {
        super(EventType.SELECT, id, so, po, tableNames);
        this.re = new ReadEvent(this, wroInverse, wherePredicate);

    }

    protected SelectEvent(SelectEvent e) {
        super(e);
    }

    @Override
    public boolean isRead() {
        return true;
    }

    @Override
    public boolean isWrite() {
        return false;
    }

    @Override
    public Event cloneEvent() {
        return new SelectEvent(this);
    }
}

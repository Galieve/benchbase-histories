package com.oltpbenchmark.apiHistory.events;

import java.util.HashSet;
import java.util.Set;

public class AbortEvent extends Event{

    public AbortEvent(Integer id, Integer so, Integer po) {
        super(EventType.ABORT, id, so, po, new HashSet<>());
    }

    @Override
    public boolean isRead() {
        return false;
    }

    @Override
    public boolean isWrite() {
        return false;
    }

    @Override
    public boolean isAbort() {
        return true;
    }
}

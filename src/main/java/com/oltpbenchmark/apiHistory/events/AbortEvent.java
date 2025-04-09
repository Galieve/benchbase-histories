package com.oltpbenchmark.apiHistory.events;

import java.util.HashSet;

public class AbortEvent extends Event{

    public AbortEvent(Integer id, Integer so, Integer po) {
        super(EventType.ABORT, id, so, po, new HashSet<>());
    }

    protected AbortEvent(AbortEvent abortEvent) {
        super(abortEvent);
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

    @Override
    public Event cloneEvent() {
        return new AbortEvent(this);
    }
}

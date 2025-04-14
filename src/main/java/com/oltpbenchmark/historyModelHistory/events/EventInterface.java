package com.oltpbenchmark.historyModelHistory.events;

public interface EventInterface {

    public boolean isRead();
    public boolean isWrite();

    public Event cloneEvent();

}

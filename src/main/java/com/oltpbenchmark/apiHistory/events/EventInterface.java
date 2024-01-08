package com.oltpbenchmark.apiHistory.events;

import java.util.Set;

public interface EventInterface {

    public boolean isRead();
    public boolean isWrite();

    public default boolean isAbort(){
        return false;
    }

    Integer getId();

    Integer getSo();

    public EventID getEventID();

    public int getPo();

    public boolean belongsInTable(Variable variable);

    public Set<String> getTableNames();


}

package com.oltpbenchmark.apiHistory.events;

import java.util.Set;

public abstract class Event implements EventInterface {

    protected enum EventType{SELECT, INSERT, DELETE, UPDATE, ABORT, INITIALABSENT};

    protected EventID eventID;
    protected EventType eventType;

    protected Integer id;

    protected Integer so;

    protected Integer po;

    protected Set<String> tableNames;

    protected Event(EventType eventType, Integer id, Integer so, Integer po, Set<String> tableNames) {
        this.id = id;
        this.so = so;
        this.po = po;
        this.eventID = new EventID(id, so , po);
        this.eventType = eventType;
        this.tableNames = tableNames;
    }

    @Override
    public final Integer getId() {
        return id;
    }

    @Override
    public final Integer getSo() {
        return so;
    }

    @Override
    public final EventID getEventID() {
        return eventID;
    }

    @Override
    public final int getPo() {
        return po;
    }

    @Override
    public boolean belongsInTable(Variable variable) {
        for(var t: tableNames){
            if(variable.getStringName().contains(t)){
                return true;
            }
        }
        return false;
    }

    @Override
    public Set<String> getTableNames() {
        return tableNames;
    }
}

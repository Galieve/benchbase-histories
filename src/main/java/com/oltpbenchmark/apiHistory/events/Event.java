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

    protected ReadEvent re;

    protected WriteEvent we;

    protected Event(EventType eventType, Integer id, Integer so, Integer po, Set<String> tableNames) {
        this.id = id;
        this.so = so;
        this.po = po;
        this.eventID = new EventID(id, so , po);
        this.eventType = eventType;
        this.tableNames = tableNames;
        this.re = null;
        this.we = null;
    }

    protected Event(Event e){
        this.id = e.id;
        this.so = e.so;
        this.po = e.po;
        this.eventID = e.eventID;
        this.eventType = e.eventType;
        this.tableNames = e.tableNames;
        this.re = e.re;
        this.we = e.we;
    }

    public final Integer getId() {
        return id;
    }

    public final Integer getSo() {
        return so;
    }

    public final EventID getEventID() {
        return eventID;
    }

    public final int getPo() {
        return po;
    }

    public boolean belongsInTable(Variable variable) {
        for(var t: tableNames){
            if(variable.getStringName().contains(t + ",")){
                return true;
            }
        }
        return false;
    }

    public Set<String> getTableNames() {
        return tableNames;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder();
        if(re != null){
            sb.append(re).append("\n");
        }
        if(we != null){
            sb.append(we).append("\n");
        }
        return sb.toString();
    }

    public boolean isAbort(){
        return false;
    }

    public ReadEvent getReadEvent() {
        return re;
    }

    public WriteEvent getWriteEvent() {
        return we;
    }

}

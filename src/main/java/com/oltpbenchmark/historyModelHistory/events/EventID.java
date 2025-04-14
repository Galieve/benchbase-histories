package com.oltpbenchmark.historyModelHistory.events;

import java.util.Objects;

public class EventID{
    protected String id;

    public EventID(Integer id, Integer so, Integer po) {
        this.id = generateID(id, so, po);
    }

    public EventID(String writeid) {
        this.id = writeid;
    }

    public static String generateID(int id, int so, int po){
        return id + "," + so + "," + po;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EventID eventID)) return false;
        return Objects.equals(id, eventID.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "EventID{" +
               "id='" + id + '\'' +
               '}';
    }
}
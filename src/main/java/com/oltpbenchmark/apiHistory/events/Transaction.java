package com.oltpbenchmark.apiHistory.events;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Transaction implements Iterable<Event>{

    protected Integer id;

    protected Integer so;

    protected ArrayList<Event> events;

    protected Map<Variable, Pair<WriteEvent, Value>> writeSet;

    protected IsolationLevel isolationLevel;

    public Transaction(ArrayList<Event> events, Integer id, Integer so, IsolationLevel iso) {
        this.events = events;
        this.id = id;
        this.so = so;
        this.isolationLevel = iso;
        this.writeSet = null;
    }

    public Integer getId() {
        return id;
    }

    public Integer getSo() {
        return so;
    }

    public ArrayList<Event> getEvents() {
        return events;
    }

    @Override
    public Iterator<Event> iterator() {
        return events.iterator();
    }

    public Event get(int index){
        return events.get(index);
    }

    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent e, Variable x) {
        return isolationLevel.satisfyConstraint(h, co, t2, e, x);
    }

    private void computeWriteSet(){
        writeSet = new HashMap<>();
        if(events.isEmpty()) return;
        if(isAborted()) return;

        var reverseIterator = events.listIterator(events.size());
        while (reverseIterator.hasPrevious()) {
            var e = reverseIterator.previous();
            if(!e.isWrite()) continue;
            var w = e.getWriteEvent();
            var ws = w.getWriteSet().entrySet();
            for(var p : ws){
                if(writeSet.containsKey(p.getKey())) continue;
                writeSet.put(p.getKey(), new Pair<>(w, p.getValue()));
            }
        }
    }

    public Map<Variable, Pair<WriteEvent, Value>> getWriteSet(){
        if(writeSet == null){
            computeWriteSet();
        }
        return writeSet;
    }

    public void resetWriteSet(){
        writeSet = null;
    }


    public boolean isAborted() {
        return events.size() > 0 && events.get(events.size() - 1).isAbort();
    }

    @Override
    public String toString() {
        return "Transaction{" +
               "id=" + id +
               ", so=" + so +
               ", events=" + events +
               ", writeSet=" + writeSet +
               ", isolationLevel=" + isolationLevel +
               '}';
    }
}

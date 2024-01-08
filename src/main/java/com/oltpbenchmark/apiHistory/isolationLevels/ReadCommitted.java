package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;

import java.sql.Connection;
import java.util.ArrayList;

public class ReadCommitted extends IsolationLevel{

    protected static ReadCommitted instance;

    protected ReadCommitted(){

    }

    public static IsolationLevel getIsolationLevel() {
        if(instance == null){
            instance = new ReadCommitted();
        }
        return instance;
    }

    @Override
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent r, Variable x) {

        var t3 = h.getTransactions().get(r.getId()).get(r.getSo());

        for(var e: t3){
            if(e.getPo() == r.getPo()) return false;
            if(!e.isRead()) continue;
            var s = (ReadEvent) e;
            if(h.areSOUWRRelated(t2, s)) return true;
        }
        return false;
    }

    @Override
    public int getMode() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }
}

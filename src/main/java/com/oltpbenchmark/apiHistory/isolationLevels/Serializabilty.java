package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map;

public class Serializabilty extends IsolationLevel{

    protected static Serializabilty instance;

    private Serializabilty(){

    }

    @Override
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent r, Variable x) {
        var translator = h.getTranslator();
        var t3 = h.getTransactions().get(r.getId()).get(r.getSo());
        var i = translator.get(t2);
        var j = translator.get(t3);
        return co.get(i).get(j);
    }

    protected static IsolationLevel getIsolationLevel() {
        if(instance == null){
            instance = new Serializabilty();
        }
        return instance;
    }

    @Override
    public int getMode() {
        return Connection.TRANSACTION_SERIALIZABLE;
    }
}

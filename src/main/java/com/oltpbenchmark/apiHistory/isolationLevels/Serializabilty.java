package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.prefix.PrefixHistory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Map;

public class Serializabilty implements IsolationLevel{

    protected static Serializabilty instance;

    private Serializabilty(){

    }

    @Override
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, Transaction t3) {
        var translator = h.getTranslator();
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

    @Override
    public boolean hasTransactionalAxioms() {
        return true;
    }

    @Override
    public boolean isPredicateExtensible(PrefixHistory p, ArrayList<ArrayList<Boolean>> co, Transaction t, Transaction t3) {
        var h = p.getHistory();
        var wro = h.getWroPerTransaction();
        //t3 does not contain any read event.
        if(!wro.containsKey(t3)) return true;

        for(var x : t.getWriteSet().keySet()){
            if(!wro.get(t3).containsKey(x)) continue;
            //wro_x^{-1}(r) \ downarrow
            var t1 = wro.get(t3).get(x);
            //t1 \in p
            if(!p.contains(t1)) continue;
            if(satisfyConstraint(h, co, t, t3)) return false;

        }
        return true;
    }
}

package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.prefix.PrefixHistory;

import java.sql.Connection;
import java.util.ArrayList;

public class ReadCommitted implements IsolationLevel{

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
            var s = e.getReadEvent();
            if(h.areSOUWRRelated(t2, s)) return true;
        }
        return false;
    }

    @Override
    public int getMode() {
        return Connection.TRANSACTION_READ_COMMITTED;
    }

    @Override
    public boolean hasTransactionalAxioms() {
        return false;
    }

    @Override
    public boolean isPredicateExtensible(PrefixHistory p, ArrayList<ArrayList<Boolean>> co, Transaction t, Transaction t3) {
        var h = p.getHistory();
        var wro = h.getWro();
        var transactions = h.getTransactions();
        //t3 does not contain any read event.
        //if(!wro.containsKey(t3)) return true;

        for(var x : t.getWriteSet().keySet()){
            for(var e: t){
                if(e.isRead()){
                    var r = e.getReadEvent();
                    //wro_x^{-1}(r) \ downarrow
                    if(!wro.get(x).containsKey(r)) continue;
                    var w = wro.get(x).get(r);
                    var t1 = transactions.get(w.getId()).get(w.getSo());
                    //t1 \in p
                    if(!p.contains(t1)) continue;
                    if(satisfyConstraint(h, co, t, r, x)) return false;
                }
            }
        }
        return true;
    }
}

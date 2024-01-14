package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;

import java.sql.Connection;
import java.util.ArrayList;

public class SnapshotIsolation extends IsolationLevel{

    protected static SnapshotIsolation instance;

    protected SnapshotIsolation(){

    }

    public static IsolationLevel getIsolationLevel() {
        if(instance == null){
            instance = new SnapshotIsolation();
        }
        return instance;
    }

    @Override
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent r, Variable x) {

        var t3 = h.getTransactions().get(r.getId()).get(r.getSo());
        var i = h.getTranslator().get(t2);
        var j = h.getTranslator().get(t3);

        for(var ses : h.getTransactions()){
            for(var t4 : ses){
                var k = h.getTranslator().get(t4);
                if(co.get(i).get(k) || i.equals(k)){
                    //Prefix
                    if(h.areSOUWRRelated(t4, t3)) return true;
                    //Conflict
                    if(co.get(k).get(j)){
                        //They write a common variable.
                        for(var key : t3.getWriteSet().keySet()){
                            if(t4.getWriteSet().containsKey(key)) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    @Override
    public int getMode() {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }
}

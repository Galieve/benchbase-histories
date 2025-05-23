package com.oltpbenchmark.historyModelHistory.isolationLevels;

import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.algorithmsHistory.prefix.PrefixHistory;
import com.oltpbenchmark.algorithmsHistory.prefix.PrefixTM;

import java.sql.Connection;
import java.util.ArrayList;

public class SnapshotIsolation implements IsolationLevel{

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
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, Transaction t3) {

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

    @Override
    public boolean hasTransactionalAxioms() {
        return true;
    }

    @Override
    public boolean isPredicateExtensible(PrefixHistory p, ArrayList<ArrayList<Boolean>> co, Transaction t, Transaction t3) {
        var h = p.getHistory();
        var wro = h.getWroPerTransaction();
        var translator = h.getTranslator();
        //t3 does not contain any read event.
        if(!wro.containsKey(t3)) return true;
        for(var t_ : translator.keySet()) {
            if (!p.contains(t_) && t_ != t) continue;
            for (var x : t_.getWriteSet().keySet()) {
                //wro_x^{-1}(r) \ downarrow
                if (!wro.get(t3).containsKey(x)) continue;

                var t1 = wro.get(t3).get(x);

                if(!(p instanceof PrefixTM pTM)) throw new IllegalArgumentException("prefix class' must be " + PrefixTM.class);
                //t1 != M(x)
                //if p does not contain init (all session 0), M(x) is not defined. You should extend in this case, I guess, because this only triggers for the init case.
                if (pTM.getLast(x) == t1 || pTM.getLast(x) == null) continue;
                if (satisfyConstraint(h, co, t_, t3)) return false;

            }
        }
        return true;
    }
}

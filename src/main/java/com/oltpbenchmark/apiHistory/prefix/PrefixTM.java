package com.oltpbenchmark.apiHistory.prefix;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.util.HistoryUtil;

import java.util.HashMap;

public class PrefixTM extends PrefixHistory {

    protected HashMap<Variable, Transaction> varsToLastTransaction;

    public PrefixTM(History h, PrefixFactory f){
        super(h, f);
        varsToLastTransaction = new HashMap<>();
    }

    public PrefixTM(PrefixTM p){
        super(p);
        varsToLastTransaction = new HashMap<>(p.varsToLastTransaction);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!super.equals(o)) return false;
        if (!(o instanceof PrefixTM that)) return false;
        return HistoryUtil.equals(varsToLastTransaction, that.varsToLastTransaction);
    }

    @Override
    public PrefixHistory clone() {
        return new PrefixTM(this);
    }

    @Override
    protected PrefixHistory extend(Transaction t) {
        var ext = clone();
        var i = t.getId();
        ext.transactions.get(i).add(t);
        ext.size++;
        for(var w: t.getWriteSet().keySet()){
            varsToLastTransaction.put(w, t);
        }
        return ext;
    }

    public Transaction getLast(Variable x){
        return varsToLastTransaction.getOrDefault(x, null);
    }
}

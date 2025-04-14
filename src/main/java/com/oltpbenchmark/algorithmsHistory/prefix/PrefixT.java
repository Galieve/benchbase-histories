package com.oltpbenchmark.algorithmsHistory.prefix;

import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.historyModelHistory.events.Transaction;

public class PrefixT extends PrefixHistory {

    public PrefixT(History h, PrefixFactory f){
        super(h, f);
    }
    public PrefixT(PrefixT p){
        super(p);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if(!super.equals(o)) return false;
        return o instanceof PrefixT;
    }

    @Override
    public PrefixHistory clone() {
        return new PrefixT(this);
    }

    @Override
    protected PrefixHistory extend(Transaction t) {
        var ext = clone();
        var i = t.getId();
        ext.transactions.get(i).add(t);
        ext.size++;
        return ext;
    }


}

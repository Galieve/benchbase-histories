package com.oltpbenchmark.algorithmsHistory.prefix;

import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.historyModelHistory.History;

import java.sql.Connection;

public class PrefixFactory {

    protected History h;
    protected Boolean SI;

    public PrefixFactory(History history) {
        h = history;
        SI = false;
        for (var ses : h.getTransactions()) {
            for (var t : ses) {
                //Repeatable Read == SI for us.
                if (t.getIsolationLevel().equals(IsolationLevel.get(Connection.TRANSACTION_REPEATABLE_READ))) {
                    SI = true;
                    return;
                }
            }
        }
    }

    public PrefixHistory extend(PrefixHistory p, Transaction t){
        var i = t.getId();
        var j = t.getSo();
        var transactions = p.getTransactions();
        if(transactions.size() <= i || transactions.get(i).size() != j) throw new IllegalArgumentException();
        return p.extend(t);
    }

    public PrefixHistory initPrefix(History history) { //history must extend PrefixHistory.h
        if(SI) return new PrefixTM(history, this);
        else return new PrefixT(history, this);
    }

    public PrefixHistory initPrefix() {
        return initPrefix(h);
    }
}

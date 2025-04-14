package com.oltpbenchmark.historyModelHistory.isolationLevels;

import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.historyModelHistory.events.ReadEvent;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.events.Variable;
import com.oltpbenchmark.algorithmsHistory.prefix.PrefixHistory;

import java.sql.Connection;
import java.util.ArrayList;

public interface IsolationLevel {

    public static Integer getMode(String isolationLevel){
        if(isolationLevel == null) return null;
        switch (isolationLevel){
            case "TRANSACTION_SERIALIZABLE":
                return Connection.TRANSACTION_SERIALIZABLE;
            case "TRANSACTION_READ_COMMITTED":
                return Connection.TRANSACTION_READ_COMMITTED;
            case "TRANSACTION_REPEATABLE_READ":
                return Connection.TRANSACTION_REPEATABLE_READ;
            case "TRANSACTION_READ_UNCOMMITTED":
                return Connection.TRANSACTION_READ_UNCOMMITTED;
            case "TRANSACTION_NONE":
                return Connection.TRANSACTION_NONE;
            default:
                return Connection.TRANSACTION_NONE;
        }

    }

    public static IsolationLevel get(Integer isolationLevel) {
        if(isolationLevel == null) return null;
        else if(isolationLevel == Connection.TRANSACTION_SERIALIZABLE) return Serializabilty.getIsolationLevel();
        else if(isolationLevel == Connection.TRANSACTION_REPEATABLE_READ) return SnapshotIsolation.getIsolationLevel();
        //return RepeatableRead.getIsolationLevel();
        else if(isolationLevel == Connection.TRANSACTION_READ_COMMITTED) return ReadCommitted.getIsolationLevel();
        //We do not accept ReadUncommitted.
        else return null;
    }

    public static IsolationLevel get(String isolationLevel){
        return get(getMode(isolationLevel));
    }

    public default boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent r, Variable x) {
        throw new IllegalCallerException();
    }

    public int getMode();

    public boolean hasTransactionalAxioms();

    public default boolean satisfyConstraint(History history, ArrayList<ArrayList<Boolean>> coLarge, Transaction t, Transaction t3){
        throw new IllegalCallerException();
    }

    public boolean isPredicateExtensible(PrefixHistory p, ArrayList<ArrayList<Boolean>> co, Transaction t, Transaction t3);
}

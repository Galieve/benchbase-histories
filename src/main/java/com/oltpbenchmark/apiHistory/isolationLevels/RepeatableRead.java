package com.oltpbenchmark.apiHistory.isolationLevels;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;

import java.sql.Connection;
import java.util.ArrayList;

public class RepeatableRead extends IsolationLevel{

    protected static RepeatableRead instance;

    protected RepeatableRead(){

    }

    public static IsolationLevel getIsolationLevel() {
        if(instance == null){
            instance = new RepeatableRead();
        }
        return instance;
    }

    @Override
    public boolean satisfyConstraint(History h, ArrayList<ArrayList<Boolean>> co, Transaction t2, ReadEvent r, Variable x) {
        return false;
    }

    @Override
    public int getMode() {
        return Connection.TRANSACTION_REPEATABLE_READ;
    }
}

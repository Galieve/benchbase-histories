package com.oltpbenchmark.apiHistory.algorithms;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.ResultHistory;

public interface ConsistencyChecker {

    public boolean checkConsistency(History history, ResultHistory result) throws InterruptedException;

}

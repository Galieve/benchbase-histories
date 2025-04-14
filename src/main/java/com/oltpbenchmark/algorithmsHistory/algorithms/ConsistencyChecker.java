package com.oltpbenchmark.algorithmsHistory.algorithms;

import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.historyModelHistory.ResultHistory;

public interface ConsistencyChecker {

    public boolean checkConsistency(History history, ResultHistory result) throws InterruptedException;

}

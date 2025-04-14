package com.oltpbenchmark.historyModelHistory;

import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.historyModelHistory.isolationLevels.IsolationLevel;

public class TransactionTypeHistory extends TransactionType {

    protected long startTime;

    protected IsolationLevel isolationLevel;

    protected TransactionTypeHistory(Class<? extends Procedure> procedureClass, int id, boolean supplemental, long preExecutionWait, long postExecutionWait, Integer isolationLevel) {
        super(procedureClass, id, supplemental, preExecutionWait, postExecutionWait);
        startTime = -1;
        this.isolationLevel = IsolationLevel.get(isolationLevel);
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public IsolationLevel getIsolationLevel() {
        return isolationLevel;
    }
}

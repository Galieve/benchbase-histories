package com.oltpbenchmark.historyModelHistory;

import com.oltpbenchmark.algorithmsHistory.algorithms.AlgorithmUtil;
import com.oltpbenchmark.algorithmsHistory.algorithms.CSOB;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.algorithmsHistory.prefix.PrefixFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;

public class FullHistory extends History{

    private static final Logger LOG = LoggerFactory.getLogger(FullHistory.class);

    public FullHistory(ArrayList<ArrayList<Transaction>> transactions) {
        super(transactions);
        LOG.info("Starting Full:<init>");

        IsolationLevel isolation = null;

        for(int i = 1; i < transactions.size(); ++i){ //We skip the initial session (init transaction)
            for(var t : transactions.get(i)){
                if(isolation == null){
                    isolation = t.getIsolationLevel(); //There must be a single isolation level.
                }
                else if(isolation != t.getIsolationLevel()){
                    throw new IllegalArgumentException(transactions.toString());
                }
            }
        }
        LOG.info("Full <init> finished!");

    }

    public FullHistory(History history) {
        super(history);
    }

    public boolean checkConsistency(ResultHistory result) throws InterruptedException {
        return checkKVConsistency(result);
    }

    protected boolean checkKVConsistency(ResultHistory result) throws InterruptedException{
        //var co = computeSoUWr();
        var co = AlgorithmUtil.computeSoUWr(this);
        var factory = new PrefixFactory(this);
        return CSOB.csob(this, co, factory.initPrefix(), new HashSet<>());
    }
}

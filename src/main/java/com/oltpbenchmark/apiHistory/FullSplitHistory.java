package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.apiHistory.algorithms.AlgorithmUtil;
import com.oltpbenchmark.apiHistory.algorithms.CSOB;
import com.oltpbenchmark.apiHistory.events.Event;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.apiHistory.prefix.PrefixFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;

public class FullSplitHistory extends FullHistory{

    private static final Logger LOG = LoggerFactory.getLogger(FullSplitHistory.class);

    public FullSplitHistory(ArrayList<ArrayList<Transaction>> transactions) {
        super(splitTransactions(transactions));
        LOG.info("Starting FullSplit:<init>");

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
        LOG.info("FullSplit <init> finished!");

    }

    private static ArrayList<ArrayList<Transaction>> splitTransactions(ArrayList<ArrayList<Transaction>> transactions){
        var split = new ArrayList<ArrayList<Transaction>>();
        int i = 0;
        for(var ses: transactions){
            split.add(new ArrayList<>());
            int j = 0;
            for(var t : ses){
                var reads = new ArrayList<Event>();
                var writes = new ArrayList<Event>();
                for(var e: t){
                    if(e.isRead()){

                    }
                }
                split.get(i).add(new Transaction(reads, i, 2*j, IsolationLevel.get(Connection.TRANSACTION_SERIALIZABLE), t.getName()));
                split.get(i).add(new Transaction(reads, i, 2*j + 1, IsolationLevel.get(Connection.TRANSACTION_SERIALIZABLE), t.getName()));
                ++j;
            }
            ++i;
        }
        return split;
    }

    public FullSplitHistory(History history) {
        super(history);
    }

    @Override
    public boolean checkConsistency( ResultHistory result) throws InterruptedException {
        return checkKVConsistency( result);
    }

    protected boolean checkKVConsistency(ResultHistory result) throws InterruptedException{
        var co = AlgorithmUtil.computeSoUWr(this);
        var factory = new PrefixFactory(this);
        return CSOB.csob(this, co, factory.initPrefix(), new HashSet<>());
    }
}

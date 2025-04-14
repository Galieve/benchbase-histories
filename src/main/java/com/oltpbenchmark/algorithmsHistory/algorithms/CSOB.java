package com.oltpbenchmark.algorithmsHistory.algorithms;

import com.oltpbenchmark.algorithmsHistory.prefix.PrefixHistory;
import com.oltpbenchmark.historyModelHistory.History;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Set;

public class CSOB {
    private static final Logger LOG = LoggerFactory.getLogger(CSOB.class);


    public static boolean csob(History h, ArrayList<ArrayList<Boolean>> co, PrefixHistory prefix, Set<PrefixHistory> seen) throws InterruptedException {
        var transactions = h.getTransactions();
        var factory = prefix.getFactory();
        if(prefix.size() == co.size()){
            return true;
        }
        for(var ses: transactions){
            for(var t: ses){
                if(Thread.interrupted()) throw new InterruptedException();
                if(prefix.contains(t)) continue;
                if(!prefix.isConsistent(co, t)) continue;
                var ext = factory.extend(prefix, t);
                if(seen.contains(ext)) continue;
                if(csob(h, co, ext, seen)) return true;
                seen.add(ext);
            }
        }
        return false;
    }


}

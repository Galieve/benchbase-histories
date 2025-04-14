package com.oltpbenchmark.algorithmsHistory.algorithms;

import com.oltpbenchmark.historyModelHistory.events.ReadEvent;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.events.Variable;
import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.utilHistory.DataStructureUtilHistory;
import com.oltpbenchmark.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AlgorithmUtil {

    private static final Logger LOG = LoggerFactory.getLogger(AlgorithmUtil.class);

    protected static boolean dfsCOAcyclic(ArrayList<ArrayList<Boolean>> rel, ArrayList<Integer> color, Integer u){
        color.set(u,1);
        for(int v = 0; v < rel.get(u).size(); ++v){
            if(u == v) continue;
            if(rel.get(u).get(v)
               && color.get(v) == 0){
                boolean b = dfsCOAcyclic(rel, color, v);
                if(!b) return false;

            }
            else if(rel.get(u).get(v)
                    && color.get(v) == 1){
                return false;
            }
        }
        color.set(u,2);
        return true;
    }


    protected static boolean isAcyclic(ArrayList<ArrayList<Boolean>> co){
        //0 = not visited, 1 = in process, 2 = finished
        int n = co.size();
        ArrayList<Integer> color = new ArrayList<>(Collections.nCopies(n,0));
        for(int i = 0; i < n; ++i){
            if(color.get(i) == 0){
                if(!AlgorithmUtil.dfsCOAcyclic(co, color, i)){
                    return false;
                }
            }
        }
        return true;
    }

    public static ArrayList<ArrayList<Boolean>> computeSoUWr(History h) {
        var soUwr = DataStructureUtilHistory.deepclone(h.getWroTransactions());
        var soTr = h.getSoTransactions();
        for(int i = 0; i < soUwr.size(); ++i){
            for(int j = 0; j < soUwr.get(i).size(); ++j){
                soUwr.get(i).set(j, soUwr.get(i).get(j) || soTr.get(i).get(j));
            }
        }
        return soUwr;
    }

    public static ArrayList<ArrayList<Boolean>> computeCOFixpoint(History h, ArrayList<ArrayList<Boolean>> coInit) throws InterruptedException {
        var co = coInit;
        int i = 0;
        do {
            coInit = co;
            co = ComputeCO.computeCO(h, coInit);
            if(Thread.interrupted()) throw new InterruptedException();
            ++i;
        }
        while (!DataStructureUtilHistory.equals(co, coInit));
        LOG.debug("#Iterations COFixpoint: " + i);
        return co;
    }

    protected static ArrayList<Integer> initIndex(List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> xh, boolean reverse){
        var index = new ArrayList<>(Collections.nCopies(xh.size(),0));
        if(reverse) {
            for (int i = 0; i < index.size(); ++i) {
                index.set(i, xh.get(i).size() - 1);
            }
        }
        return index;
    }

    protected static boolean nextSet(List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> xh, ArrayList<Integer> index, boolean reverse){
        if(reverse){
            for (int i = index.size() - 1; i >= 0; --i) {
                index.set(i, index.get(i) - 1);

                if (index.get(i) == -1) {
                    index.set(i, xh.get(i).size() -1);
                } else return true;

            }
            return false;
        }
        else {
            for (int i = index.size() - 1; i >= 0; --i) {
                index.set(i, index.get(i) + 1);

                if (index.get(i) == xh.get(i).size()) {
                    index.set(i, 0);
                } else return true;

            }
            return false;
        }
    }
}

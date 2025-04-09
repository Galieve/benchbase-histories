package com.oltpbenchmark.apiHistory.algorithms;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.ResultHistory;
import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.util.HistoryUtil;
import com.oltpbenchmark.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class NaiveCheckClientConsistency implements ConsistencyChecker {
    private static final Logger LOG = LoggerFactory.getLogger(NaiveCheckClientConsistency.class);

    @Override
    public boolean checkConsistency(History history, ResultHistory result) throws InterruptedException {
        return checkNaive(history, result, true);
    }

    protected boolean checkNaive(History history, ResultHistory resultHistory, boolean reverse) throws InterruptedException {

        var soUwr = AlgorithmUtil.computeSoUWr(history);
        var coInit = HistoryUtil.transitiveClosure(soUwr);

        var co = AlgorithmUtil.computeCOFixpoint(history, coInit);
        if(Thread.interrupted()) throw new InterruptedException();
        if(!AlgorithmUtil.isAcyclic(co)){
            LOG.debug("CO is NOT acyclic!");
            return false;
        }
        LOG.debug("CO is acyclic");

        var eh = getAllMissingWR(history, co);
        if(Thread.interrupted()) throw new InterruptedException();
        var xh = getXh(history, co, eh);
        if(Thread.interrupted()) throw new InterruptedException();

        if(eh.size() != 0 && xh.size() == 0){
            LOG.debug("No possible extension!");
            return false;
        }
        else if(xh.size() == 0){
            LOG.debug("Initial history is a full history!");
            var coMap = new HashMap<Transaction, Integer>();
            return checkNaiveFull(history, co, coMap);
        }
        else {
            var index = AlgorithmUtil.initIndex(xh, reverse);
            int numCsob = 0;
            LOG.debug("Multiple extensions");
            do {
                var h = new History(history);
                var coH = HistoryUtil.deepclone(co);
                LOG.debug("Call #" + numCsob + ": generating extension");
                var extension = true;
                for (int i = 0; i < index.size() && extension; ++i) {
                    var zeroSet = xh.get(i);
                    var p = zeroSet.get(index.get(i));
                    var variable = p.first;
                    var r = p.second.first;
                    var w = p.second.second.getWriteSet().get(variable).first;
                    h.setWR(variable, r, w);
                    extension = !r.satisfyWhere(w.getWrittenValue(variable));
                    var translator = h.getTranslator();
                    int j = translator.get(h.getTransactions().get(w.getId()).get(w.getSo()));
                    int k = translator.get(h.getTransactions().get(r.getId()).get(r.getSo()));
                    if (j != k)
                        coH.get(j).set(k, true);
                }
                if(extension){
                    coH = AlgorithmUtil.computeCOFixpoint(history, coH);
                    if (AlgorithmUtil.isAcyclic(coH)) {
                        var coMap = new HashMap<Transaction, Integer>();
                        LOG.debug("Call #" + numCsob + ": starting procedure");

                        if (checkNaiveFull(h, coH, coMap)) {
                            LOG.debug("Call #" + numCsob + ": consistent!");
                            return true;
                        }
                    } else {
                        LOG.debug("Call #" + numCsob + ": CO is NOT acyclic!");
                    }
                }
                else {
                    LOG.debug("History #" + numCsob + ": not a full extension!");
                }
                ++numCsob;
            } while (!Thread.interrupted() && AlgorithmUtil.nextSet(xh, index, reverse));
            if (Thread.interrupted()) throw new InterruptedException();
            return false;
        }
    }

    protected boolean checkNaiveFull(History h, ArrayList<ArrayList<Boolean>> conec, Map<Transaction, Integer> coMap) throws InterruptedException {
        if(Thread.interrupted()) throw new InterruptedException();

        var translator = h.getTranslator();
        var nTransactions = translator.size();
        var wro = h.getWro();
        var maxPut = coMap.size();
        var transactions = h.getTransactions();
        if(maxPut == nTransactions){
            var co = generateCO(h, coMap);
            for(var t3 : translator.keySet()){
                for(var t2 :translator.keySet()) {
                    var j = translator.get(t2);
                    for(var x : t2.getWriteSet().keySet()) {
                        for (var e : t3) {
                            if (e.isRead()) {
                                var r = e.getReadEvent();
                                var w = wro.get(x).get(r);
                                var t1 = transactions.get(w.getId()).get(w.getSo());
                                var i = translator.get(t1);
                                if (co.get(i).get(j)) {
                                    if (t3.hasTransactionalAxioms()) {
                                        if (t3.satisfyConstraint(h, co, t2)) return false;
                                        else break;
                                    } else {
                                        if (t3.satisfyConstraint(h, co, t2, r, x)) return false;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return true;
        }
        else {
            for (var t : translator.keySet()) {
                if (coMap.containsKey(t)) continue;
                var canAddT = true;
                for(var t_ : translator.keySet()){
                    if(coMap.containsKey(t_)) continue;
                    if(conec.get(translator.get(t)).get(translator.get(t_))){
                        canAddT = false; break;
                    }
                }
                if(!canAddT) continue;
                coMap.put(t, maxPut);
                if (checkNaiveFull(h, conec, coMap)) return true;
                coMap.remove(t);

            }
        }
        return false;
    }

    private ArrayList<ArrayList<Boolean>> generateCO(History h, Map<Transaction, Integer> coMap) {
        var translator = h.getTranslator();
        var nTransactions = translator.size();
        var co = new ArrayList<ArrayList<Boolean>>();
        for(int i = 0; i < nTransactions; ++i){
            co.add(new ArrayList<>());
            for(int j = 0; j < nTransactions; ++j){
                co.get(i).add(false);
            }
        }
        for(var et: translator.entrySet()){
            for(var et_ : translator.entrySet()){
                if(coMap.get(et.getKey()) < coMap.get(et_.getKey())) {
                    var i = translator.get(et.getKey());
                    var j = translator.get(et_.getKey());
                    co.get(i).set(j, true);
                }
            }
        }
        return co;
    }


    protected Set<Pair<ReadEvent, Variable>> getAllMissingWR(History h, ArrayList<ArrayList<Boolean>> co){
        var transactions = h.getTransactions();
        var wro = h.getWro();
        var eh = new HashSet<Pair<ReadEvent, Variable>>();
        for(var se: transactions){
            for(var t: se){
                for(var e: t){
                    if(!e.isRead()) continue;
                    var r = e.getReadEvent();
                    for(var variable : wro.keySet()){
                        if(!wro.get(variable).containsKey(r)){
                            eh.add(new Pair<>(r, variable));
                        }
                    }

                }
            }
        }
        return eh;
    }


    protected Set<Transaction> getZeroSet(History h, ArrayList<ArrayList<Boolean>> co, Variable x, ReadEvent r){
        var writePerVariable = h.getWritePerVariable();
        var translator = h.getTranslator();
        var transactions = h.getTransactions();
        var set = new HashSet<Transaction>();
        for(var t : writePerVariable.get(x)){
            int i = translator.get(transactions.get(r.getId()).get(r.getSo()));
            int j = translator.get(t);
            var value = t.getWriteSet().get(x).second;
            if(i == j) continue;
            if(co.get(i).get(j)) continue;
            if(!r.satisfyWhere(value)){
                set.add(t);
            }
        }
        return set;
    }


    protected List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> getXh(History h, ArrayList<ArrayList<Boolean>> co, Set<Pair<ReadEvent, Variable>> eh) throws InterruptedException {

        var xh = new ArrayList<List<Pair<Variable,Pair<ReadEvent,Transaction>>>>();
        var writePerVariable = h.getWritePerVariable();

        for (var p : eh) {

            if(Thread.interrupted()) throw new InterruptedException();

            var read = p.first;
            var variable = p.second;
            var zeroSet = getZeroSet(h, co, variable, read);
            var mappedZS = zeroSet.stream().map(t -> new Pair<>(variable, new Pair<>(read,t))).toList();
            if(mappedZS.size() == 0){
                /*LOG.debug(read.getId() + " " + read.getSo() + " "+ read.getPo() + " " + read);
                var set = writePerVariable.get(variable);
                for(var t: set){
                    if(Thread.interrupted()) throw new InterruptedException();
                    var vw = t.getWriteSet().get(variable).second;
                    if(read.satisfyWhere(vw)) {
                        LOG.debug(vw.toString());
                    }
                }


                 */
                return new ArrayList<>();
            }
            xh.add(new ArrayList<>(mappedZS));
        }
        return xh;
    }

    //We assume xh.size() == index.size()


}

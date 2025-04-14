package com.oltpbenchmark.algorithmsHistory.algorithms;

import com.oltpbenchmark.historyModelHistory.events.ReadEvent;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.events.Variable;
import com.oltpbenchmark.algorithmsHistory.prefix.PrefixFactory;
import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.historyModelHistory.ResultHistory;
import com.oltpbenchmark.utilHistory.DataStructureUtilHistory;
import com.oltpbenchmark.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class CheckSOBound implements ConsistencyChecker {

    private static final Logger LOG = LoggerFactory.getLogger(CheckSOBound.class);

    @Override
    public boolean checkConsistency(History history, ResultHistory result) throws InterruptedException {
        return checkSOBound(history, result, true);
    }

    public boolean checkSOBound(History history, ResultHistory result, boolean reverse) throws InterruptedException {
        long now = System.nanoTime();
        var factory = new PrefixFactory(history);

        var soUwr = AlgorithmUtil.computeSoUWr(history);
        var coInit = DataStructureUtilHistory.transitiveClosure(soUwr);

        var co = AlgorithmUtil.computeCOFixpoint(history, coInit);
        if(Thread.interrupted()) throw new InterruptedException();
        if(!AlgorithmUtil.isAcyclic(co)){
            LOG.debug("CO is NOT acyclic!");
            return false;
        }
        LOG.debug("CO is acyclic");

        var eh = getEh(history, co);
        if(Thread.interrupted()) throw new InterruptedException();
        var xh = getXh(history, co, eh);
        if(Thread.interrupted()) throw new InterruptedException();



        if(eh.size() != 0 && xh.size() == 0){
            LOG.debug("No possible extension!");
            return false;
        }
        else if(xh.size() == 0){
            LOG.debug("Unique CSOB call");
            var p = factory.initPrefix(history);
            return CSOB.csob(history, co, p, new HashSet<>());
        }
        else{
            var index = AlgorithmUtil.initIndex(xh, reverse);
            int numCsob = 0;
            LOG.debug("Multiple CSOB calls");
            do {
                var h = new History(history);
                var coH = DataStructureUtilHistory.deepclone(co);
                LOG.debug("Call #" + numCsob);
                var extension = true;
                for (int i = 0; i < index.size() && extension; ++i) {
                    var zeroSet = xh.get(i);
                    var p = zeroSet.get(index.get(i));
                    var variable = p.first;
                    var r = p.second.first;
                    var w = p.second.second.getWriteSet().get(variable).first;
                    extension = h.setWR(variable, r, w);
                    var translator = h.getTranslator();

                    int j = translator.get(h.getTransactions().get(w.getId()).get(w.getSo()));
                    int k = translator.get(h.getTransactions().get(r.getId()).get(r.getSo()));
                    if(j != k)
                        coH.get(j).set(k, true);
                }
                if(!extension){
                    //LOG.debug("Call #" + numCsob + ": impossible extension");
                }
                else {
                    coH = AlgorithmUtil.computeCOFixpoint(history, coH);
                    if (AlgorithmUtil.isAcyclic(coH)) {
                        var p = factory.initPrefix(h);
                        if (CSOB.csob(h, coH, p, new HashSet<>())) {
                            LOG.debug("Call #" + numCsob + ": consistent!");
                            return true;
                        }
                        else{
                            LOG.debug("Call #" + numCsob + ": not consistent!");
                        }
                    } else {
                        LOG.debug("Call #" + numCsob + ": CO is NOT acyclic!");
                    }
                }
                ++numCsob;

            }while(!Thread.interrupted() && AlgorithmUtil.nextSet(xh, index, reverse));
            if(Thread.interrupted()) throw new InterruptedException();
            return false;
        }
    }


    protected boolean isOneSetEmpty(History h, ArrayList<ArrayList<Boolean>> co, Variable x, ReadEvent r){
        var transactions = h.getTransactions();
        var translator = h.getTranslator();
        for(var ses: transactions){
            for(var t: ses){
                if(t.isAborted()) continue;
                var ws = t.getWriteSet();
                int i = translator.get(transactions.get(r.getId()).get(r.getSo()));
                int j = translator.get(t);
                if(!ws.containsKey(x)) continue;
                var value = ws.get(x).second;
                if(i == j) continue;
                if(co.get(i).get(j)) continue;
                if(r.satisfyWhere(value)) return false;
            }
        }
        return true;
    }

    //\Let $E_h = \{(r,x) \ | \ r \in \readOp{h}, x \in \Vars. \wro_x^{-1}(r) \ uparrow $ and $\mathtt{1}_r^x(\co) \neq \emptyset\}$
    protected Set<Pair<ReadEvent, Variable>> getEh(History h, ArrayList<ArrayList<Boolean>> co){
        var transactions = h.getTransactions();
        var variablePerTable = h.getVariablePerTable();
        var wro = h.getWro();
        var eh = new HashSet<Pair<ReadEvent, Variable>>();
        for(var se: transactions){
            for(var t: se){
                for(var e: t){
                    if(!e.isRead()) continue;
                    var r = e.getReadEvent();
                    var tableNames = e.getTableNames();
                    for(var tableName : tableNames) {
                        for (var variable : variablePerTable.get(tableName)) {
                            if(!e.belongsInTable(variable)) continue;
                            if(!wro.get(variable).containsKey(r) && !isOneSetEmpty(h, co, variable, r)){
                                eh.add(new Pair<>(r, variable));
                            }
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
               /* LOG.debug(read.getId() + " " + read.getSo() + " "+ read.getPo() + " " + read);
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

package com.oltpbenchmark.algorithmsHistory.algorithms;

import com.oltpbenchmark.historyModelHistory.History;
import com.oltpbenchmark.utilHistory.DataStructureUtilHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class ComputeCO {

    private static final Logger LOG = LoggerFactory.getLogger(ComputeCO.class);

    public static ArrayList<ArrayList<Boolean>> computeCO(History h, ArrayList<ArrayList<Boolean>> rInit){
        var co = DataStructureUtilHistory.deepclone(rInit);
        var transactions = h.getTransactions();
        var translator = h.getTranslator();
        var wroPerTransaction = h.getWroPerTransaction();
        var wro = h.getWro();
        for(var t2: translator.keySet()){
            int i = translator.get(t2);
            for(var t3 : translator.keySet()){
                if(t2 == t3) continue;
                if (t3.hasTransactionalAxioms()) {
                    for (var variable : t2.getWriteSet().keySet()) {
                        if (wroPerTransaction.containsKey(t3) && wroPerTransaction.get(t3).containsKey(variable)) {
                            var t1 = wroPerTransaction.get(t3).get(variable);
                            int j = translator.get(t1);
                            if (i == j) continue;
                            if (co.get(i).get(j)) continue;
                            if (t3.satisfyConstraint(h, rInit, t2)) {
                                co.get(i).set(j, true);
                            }
                        }
                    }

                } else {
                    for (var variable : t2.getWriteSet().keySet()) {
                        for (var e : t3) {
                            if (!e.isRead()) continue;
                            var r = e.getReadEvent();
                            var w = wro.get(variable).get(r);
                            if(w == null) continue; //wro_x^{-1}(r) \ uparrow
                            var t1 = transactions.get(w.getId()).get(w.getSo());
                            int j = translator.get(t1);
                            if (i == j) continue;
                            if (co.get(i).get(j)) continue;
                            if (t3.satisfyConstraint(h, rInit, t2, r, variable)) {
                                co.get(i).set(j, true);
                            }
                        }
                    }
                }
            }
        }



        return co;

    }


}

/*
for (var ses : transactions) {
            for (var t2 : ses) {
                int i = translator.get(t2);
                for(var  : ses)

                for (var es : wroPerTransaction.entrySet()) {
                    var t3 = es.getKey();
                    if (t3.hasTransactionalAxioms()) {
                        for (var variableES : t2.getWriteSet().entrySet()) {
                            var variable = variableES.getKey();
                            if (wroPerTransaction.containsKey(t3) && wroPerTransaction.get(t3).containsKey(variable)) {
                                var t1 = wroPerTransaction.get(t3).get(variable);
                                int j = translator.get(t1);
                                if (i == j) continue;
                                if (co.get(i).get(j)) continue;
                                if (t3.satisfyConstraint(h, rInit, t2)) {
                                    co.get(i).set(j, true);
                                }
                            }
                        }

                    } else {
                        for (var variable : t2.getWriteSet().keySet()) {
                            for (var e : t3) {
                                if (!e.isRead()) continue;
                                var r = e.getReadEvent();
                                var w = wro.get(variable).get(r);
                                var t1 = transactions.get(w.getId()).get(w.getSo());
                                int j = translator.get(t1);
                                if (i == j) continue;
                                if (co.get(i).get(j)) continue;
                                if (t3.satisfyConstraint(h, rInit, t2, r, variable)) {
                                    co.get(i).set(j, true);
                                }
                            }
                        }
                    }
                }
            }
        }
 */
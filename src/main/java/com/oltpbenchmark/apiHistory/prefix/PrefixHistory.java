package com.oltpbenchmark.apiHistory.prefix;

import com.oltpbenchmark.apiHistory.History;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.util.HistoryUtil;

import java.util.ArrayList;
import java.util.Objects;
import java.util.stream.Stream;

public abstract class PrefixHistory {

    protected History history;

    protected ArrayList<ArrayList<Transaction>> transactions;

    protected int size;

    protected PrefixFactory factory;

    public PrefixHistory(History history, PrefixFactory factory) {
        this.history = history;
        this.transactions = new ArrayList<>(
            Stream.generate(ArrayList<Transaction>::new)
                .limit(history.width())
                .toList());
        this.size = 0;
        this.factory = factory;
    }
    public PrefixHistory(PrefixHistory prefixHistory) {
        history = prefixHistory.history;
        transactions = HistoryUtil.deepclone(prefixHistory.transactions);
        size = prefixHistory.size;
        factory = prefixHistory.factory;
    }

    public abstract PrefixHistory clone();

    public int size(){
        return size;
    }

    public boolean contains(Transaction t) {
        var i = t.getId();
        var j = t.getSo();
        return transactions.get(i).size() > j && transactions.get(i).get(j) == t;
    }


    // $\overline{\co} = \co \cup  \{(t', t'') \ | \ t' \in (T' \cup \{t\}), t'' \in T \setminus (T \cup \{t\})\}$
    private ArrayList<ArrayList<Boolean>> enlarge(ArrayList<ArrayList<Boolean>> co, Transaction t) {
        var coLarge = HistoryUtil.deepclone(co);
        var translator = history.getTranslator();
        for(var ses : transactions) {
            for (var t1 : ses) {
                if(contains(t1)){
                    coLarge.get(translator.get(t1)).set(translator.get(t), true);
                }
                //t1 \not\in P \cup \{t\}
                else if(t1 != t){
                    coLarge.get(translator.get(t)).set(translator.get(t1), true);
                }
            }
        }
        return coLarge;
    }

    /*

 \item for every transaction $t'$ s.t. $(t', t) \in \co$ then $t' \in T'$ and,

\item for every axiom $\phi \in I_T(\tup{h, \overline{\co}}, t)$ and every variable $x$ written by $t$ there is no transaction $t_1 \in T'$ and $\iread$ event $r$ s.t. $\trans{r} \in T \setminus (T' \cup \{t\})$, $(t_1, r) \in \wro_x$ and $\axiomConstraint(\phi)(t, r, x)$
     */
    public boolean isConsistent(ArrayList<ArrayList<Boolean>> co, Transaction t) {
        var translator = history.getTranslator();
        //Condition 1 (t_ == t')
        for (var sesh : history.getTransactions()) {
            for (var t_ : sesh) {
                var i = translator.get(t_);
                var j = translator.get(t);
                //t' \not\in P
                if(t_.getSo() < transactions.get(t_.getId()).size()) continue;
                if (co.get(i).get(j)) return false;
            }
        }
        //Condition 2 t3 = trans(r)
        for (var sesh : history.getTransactions()) {
            for (var t3 : sesh) {
                //(t3 not in P \cup \{t\})
                if (t3.getSo() < transactions.get(t3.getId()).size() || t3 == t) continue;


                var colarge = enlarge(HistoryUtil.deepclone(co), t);
                if(!t3.getIsolationLevel().isPredicateExtensible(this, colarge, t, t3)) return false;
                //if !vp(v, p, t, r) => return false

            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PrefixHistory that)) return false;
        return size == that.size && Objects.equals(history, that.history) &&
               HistoryUtil.equals(transactions, that.transactions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(history, transactions, size);
    }

    public History getHistory() {
        return history;
    }

    protected abstract PrefixHistory extend(Transaction t);

    public PrefixFactory getFactory() {
        return factory;
    }

    public ArrayList<ArrayList<Transaction>> getTransactions() {
        return transactions;
    }
}

/*
 //if exists x \in keys

                if(t3.hasTransactionalAxioms()){
                    var wroPT = history.getWroPerTransaction();
                    boolean needChecking = false;
                    for(var variable : t.getWriteSet().entrySet()){
                        if(wroPT.containsKey(t3) && wroPT.get(t3).containsKey(variable.getKey())){
                            var tw = wroPT.get(t3).get(variable.getKey());
                            if(transactions.get(tw.getId()).size() > tw.getSo()) {
                                needChecking = true;
                                break;
                            }
                        }
                    }
                    if(needChecking && t3.satisfyConstraint(history,coLarge, t)){
                        return false;
                    }
                }
                else {
                    for (var p : t.getWriteSet().entrySet()) {
                        var variable = p.getKey();
                        for (var e : t3) {
                            if (!e.isRead()) continue;
                            var r = e.getReadEvent();
                            var w = history.getWro().get(variable).get(r);
                            if (w == null) continue;
                            if (transactions.get(w.getId()).size() <= w.getSo()) continue;
                            if (t3.satisfyConstraint(history, coLarge, t, r, variable)) return false;
                        }
                    }
                }
 */
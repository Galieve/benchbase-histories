package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;

import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public class PrefixHistory {

    protected History history;

    protected ArrayList<ArrayList<Transaction>> transactions;

    protected int size;

    public PrefixHistory(History history) {
        this.history = history;
        this.transactions = new ArrayList<>(
            Stream.generate(ArrayList<Transaction>::new)
                .limit(history.width())
                .toList());
        this.size = 0;
    }

    public PrefixHistory(PrefixHistory prefixHistory) {
        history = prefixHistory.history;
        transactions = HistoryUtil.deepclone(prefixHistory.transactions);
        size = prefixHistory.size;
    }

    public int size(){
        return size;
    }

    public boolean contains(Transaction t) {
        var i = t.getId();
        var j = t.getSo();
        return transactions.get(i).size() > j && transactions.get(i).get(j) == t;
    }

    public PrefixHistory extend(Transaction t) {
        var i = t.getId();
        var j = t.getSo();
        if(transactions.size() <= i || transactions.get(i).size() != j) throw new IllegalArgumentException();
        var ext = new PrefixHistory(this);
        ext.transactions.get(i).add(t);
        ext.size++;
        return ext;
    }

// $\overline{\co} = \co \cup  \{(t', t'') \ | \ t' \in (T' \cup \{t\}), t'' \in T \setminus (T \cup \{t\})\}$
    private ArrayList<ArrayList<Boolean>> enlarge(ArrayList<ArrayList<Boolean>> co, Transaction t) {
        var coLarge = HistoryUtil.deepclone(co);
        var translator = history.getTranslator();
        for(var ses1 : transactions){
            for(var t1 : ses1){
                var T = history.getTransactions();
                for(var ses2 : T) {
                    for(var t2: ses2){
                        if(transactions.get(t2.getId()).size() > t2.getSo() || t2 == t) continue;
                        coLarge.get(translator.get(t1)).set(translator.get(t2), true);
                    }
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
        var coLarge = enlarge(co, t);
        var translator = history.getTranslator();
        for (var sesh : history.getTransactions()) {
            for (var t3 : sesh) {

                //Condition 1 (t3 plays role of t')
                var i = translator.get(t3);
                var j = translator.get(t);
                if(co.get(i).get(j) &&
                   transactions.get(t3.getId()).size() <= t3.getSo()) return false;

                //Condition 2
                //(t3 not in T' \cup \{t\})
                if (transactions.get(t3.getId()).size() > t3.getSo() || t3 == t) continue;

                for(var p : t.getWriteSet().entrySet()){
                    var variable = p.getKey();
                    for (var e : t3) {
                        if (!e.isRead()) continue;
                        var r = e.getReadEvent();
                        var w = history.getWro().get(variable).get(r);
                        if(w == null) continue;
                        if(transactions.get(w.getId()).size() <= w.getSo()) continue;
                        if(t3.satisfyConstraint(history,coLarge,t,r,variable)) return false;
                    }
                }
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
}

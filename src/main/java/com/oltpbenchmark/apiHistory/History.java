package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.apiHistory.util.HistoryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class History {

    protected ArrayList<ArrayList<Transaction>> transactions;

    //Read -> Write
    protected Map<Variable,Map<ReadEvent, WriteEvent>> wro;

    //Structure only used for strong isolation levels in PrefixHistory! Do not use it anywhere else!
    protected Map<Transaction, Map<Variable, Transaction>> wroPerTransaction;

    protected Map<EventID, WriteEvent> writesIDs;

    protected ArrayList<ArrayList<Boolean>> wroTransactions;

    protected ArrayList<ArrayList<ArrayList<Boolean>>> wroReadTransactions;

    protected ArrayList<ArrayList<Boolean>> soTransactions;

    protected Map<Transaction, Integer> translator;

    protected Map<String, Set<Variable>> variablePerTable;

    protected Map<Variable, Set<Transaction>> writePerVariable;

    private static final Logger LOG = LoggerFactory.getLogger(History.class);

    public History(ArrayList<ArrayList<Transaction>> transactions) {
        LOG.info("Starting <init>");
        this.transactions = transactions;
        this.writesIDs = computeWritesID();
        this.wro = computeWR();
        addAbsentInitialRows();
        this.translator = computeTranslator();
        this.wroTransactions = translateWR();
        this.wroReadTransactions = translateWRRead();
        this.wroPerTransaction = computeWRTransaction();
        this.soTransactions = translateSO();
        this.variablePerTable = computePartition();
        this.writePerVariable = computeWritesPerVariable();
        LOG.info("<init> finished!");
    }
    public History(History history) {
        this.transactions = history.transactions;
        this.writesIDs = history.writesIDs;
        this.wro = HistoryUtil.deepclone(history.wro);
        this.translator = history.translator;
        this.wroTransactions = HistoryUtil.deepclone(history.wroTransactions);
        this.soTransactions = HistoryUtil.deepclone(history.soTransactions);
        this.variablePerTable = HistoryUtil.deepcloneSet(history.variablePerTable);
        this.writePerVariable = HistoryUtil.deepcloneSet(history.writePerVariable);
        this.wroReadTransactions = HistoryUtil.deepclone(history.wroReadTransactions);
        this.wroPerTransaction = HistoryUtil.deepclone(history.wroPerTransaction);
    }

    /*
    public boolean checkConsistency(ResultHistory result) throws InterruptedException {
        return CheckSOBound.checkSOBound(this, result);
    }
    */

    public int width() {
        return transactions.size();
    }

    public ArrayList<ArrayList<Transaction>> getTransactions() {
        return transactions;
    }

    public Map<Variable, Map<ReadEvent, WriteEvent>> getWro() {
        return wro;
    }


    public Map<Transaction, Map<Variable, Transaction>> getWroPerTransaction() {
        return wroPerTransaction;
    }

    public Map<Transaction, Integer> getTranslator() {
        return translator;
    }

    private HashMap<EventID, WriteEvent> computeWritesID() {
        var writes = new HashMap<EventID, WriteEvent>();
        for(var ses: transactions){
            for(var t: ses){
                for(var e : t){
                    if(!e.isWrite()) continue;
                    var w = e.getWriteEvent();
                    writes.put(w.getEventID(), w);
                }
            }
        }
        return writes;
    }
    private Map<Variable, Map<ReadEvent, WriteEvent>> computeWR() {
        var wro = new HashMap<Variable, Map<ReadEvent, WriteEvent>>();
        for(var ses: transactions){
            for(var t: ses){
                for(var e: t){
                    if(e.isRead()) {
                        var r = e.getReadEvent();
                        var wror = r.getWroInverse();
                        for (var p : wror.entrySet()) {
                            wro.putIfAbsent(p.getKey(), new HashMap<>());
                            wro.get(p.getKey()).put(r, writesIDs.get(p.getValue()));
                        }
                    }
                    if(!t.isAborted() && e.isWrite()){
                        var w = e.getWriteEvent();
                        for(var ws: w.getWriteSet().entrySet()){
                            wro.putIfAbsent(ws.getKey(), new HashMap<>());
                        }
                    }
                }
            }
        }
        return wro;
    }

    private Map<Transaction, Map<Variable, Transaction>> computeWRTransaction() {
        var wroPT = new HashMap<Transaction, Map<Variable, Transaction>>();
        for(var wroES : wro.entrySet()){
            var variable = wroES.getKey();
            for(var wrES : wroES.getValue().entrySet()){
                var r = wrES.getKey();
                var w = wrES.getValue();
                var tr = transactions.get(r.getId()).get(r.getSo());
                var tw = transactions.get(w.getId()).get(w.getSo());
                //Local reads should not be added!
                if(tr != tw) {
                    wroPT.putIfAbsent(tr, new HashMap<>());
                    wroPT.get(tr).put(variable, tw);
                }
                //NOTE: This method is wrong for weak isolation levels such as RC or RR where transactions may read the same variable from different transactions in two different reads!

            }
        }
        return wroPT;
    }



    private void addAbsentInitialRows() {
        var writeSet = new HashMap<Variable, Value>();
        for(var variable : wro.keySet()){
            boolean present = false;
            for(var t : transactions.get(0)){
                if(t.getWriteSet().containsKey(variable)){
                    present = true;
                    break;
                }
            }
            if(!present){
                writeSet.putIfAbsent(variable, null);
            }
        }
        transactions.get(0).get(0).resetWriteSet();
        var events = transactions.get(0).get(0).getEvents();
        var size = events.size();
        var initEvent = new InitialAbsentEvent(0,0, size, writeSet);
        events.add(initEvent);
        writesIDs.put(new EventID(0,0,size), initEvent.getWriteEvent());
        transactions.get(0).get(0).resetWriteSet();
    }


    public boolean setWR(Variable variable, ReadEvent r, WriteEvent w) {
        wro.get(variable).put(r, w);
        var tr = transactions.get(r.getId()).get(r.getSo());
        var tw = transactions.get(w.getId()).get(w.getSo());
        if(tr.hasTransactionalAxioms()
           && wroPerTransaction.containsKey(tr)
           && wroPerTransaction.get(tr).containsKey(variable) &&
           wroPerTransaction.get(tr).get(variable) != tw) return false;
        wroPerTransaction.putIfAbsent(tr, new HashMap<>());
        wroPerTransaction.get(tr).putIfAbsent(variable, tw);
        return true;
    }
    private HashMap<Transaction, Integer> computeTranslator(){
        int i = 0;
        var translator = new HashMap<Transaction, Integer>();
        for(var ses: transactions){
            for(var t: ses){
                translator.put(t, i);
                ++i;
            }
        }
        return translator;
    }

    private ArrayList<ArrayList<Boolean>> translateSO() {
        var so = new ArrayList<ArrayList<Boolean>>();
        for(var t : translator.keySet()){
            so.add(new ArrayList<>(Collections.nCopies(translator.keySet().size(), false)));
        }
        //initial transactions
        for(var t1 : transactions.get(0)){
            var i = translator.get(t1);
            for(int jdx = 1; jdx < transactions.size(); ++jdx){
                for(var t2: transactions.get(jdx)){
                    var j = translator.get(t2);
                    so.get(i).set(j, true);
                }
            }
        }

        for(var ses: transactions){
            for(var t1 : ses){
                var i = translator.get(t1);
                for(int jdx = t1.getSo() + 1; jdx < ses.size(); ++jdx){
                    var t2 = ses.get(jdx);
                    var j = translator.get(t2);
                    so.get(i).set(j, true);
                }
            }
        }
        return so;
    }

    private ArrayList<ArrayList<Boolean>> translateWR() {

        var wr = new ArrayList<ArrayList<Boolean>>();
        for(int i = 0; i < translator.keySet().size(); ++i){
            wr.add(new ArrayList<>(Collections.nCopies(translator.keySet().size(), false)));
        }
        for(var p : wro.entrySet()){
            for(var e : p.getValue().entrySet()){
                var r = e.getKey();
                var w = e.getValue();
                int i = translator.get(transactions.get(w.getId()).get(w.getSo()));
                int j = translator.get(transactions.get(r.getId()).get(r.getSo()));

                if(i != j) {
                    wr.get(i).set(j, true);
                }

            }
        }
        return wr;
    }

    private ArrayList<ArrayList<ArrayList<Boolean>>> translateWRRead() {

        var wr = new ArrayList<ArrayList<ArrayList<Boolean>>>();
        for(int i = 0; i < translator.keySet().size(); ++i){
            wr.add(new ArrayList<>());
            int j = 0;
            for(var ses: transactions){
                for(var t: ses){
                    wr.get(i).add(new ArrayList<>());
                    for(var e : t){
                        wr.get(i).get(j).add(false);
                    }
                    ++j;
                }
            }
        }
        for(var p : wro.entrySet()){
            for(var e : p.getValue().entrySet()){
                var r = e.getKey();
                var w = e.getValue();
                int i = translator.get(transactions.get(w.getId()).get(w.getSo()));
                int j = translator.get(transactions.get(r.getId()).get(r.getSo()));

                if(i != j) {
                    wr.get(i).get(j).set(r.getPo(), true);
                }

            }
        }
        return wr;
    }

    private Map<String, Set<Variable>> computePartition(){
        var vpt = new HashMap<String, Set<Variable>>();
        for(var ses: transactions){
            for(var t: ses){
                for(var e : t){
                    for(var tableName : e.getTableNames()){
                        vpt.putIfAbsent(tableName, new HashSet<>());
                    }
                }
            }
        }

        for(var varES : wro.entrySet()){
            var variable = varES.getKey();
            for(var tableName : vpt.keySet()){
                if(variable.getStringName().contains(tableName)) {
                    vpt.get(tableName).add(variable);
                }
            }
        }
        return vpt;
    }

    private Map<Variable, Set<Transaction>> computeWritesPerVariable(){
        var wpv = new HashMap<Variable, Set<Transaction>>();
        for(var ses : transactions){
            for(var t: ses){
                for(var variable : t.getWriteSet().keySet()){
                    wpv.putIfAbsent(variable, new HashSet<>());
                    wpv.get(variable).add(t);
                }
            }
        }
        return wpv;
    }

    /*

    \begin{algorithmic}[1]
\Function{\checksobound}{$\hist = \tup{T, \so, \wro}, I_T$}

\Let $\co = \mathsf{FIX}(\lambda x: \textsc{compute-}\co(h, x, I_T))(\so \cup \wro)^+$
\label{algorithm:csob:co}

\Let $E_h = \{(r,x) \ | \ r \in \readOp{h}, x \in \Vars. \wro_x^{-1}(r) \ uparrow $ and $\mathtt{1}_r^x(\co) \neq \emptyset\}$
\label{algorithm:csob:eh}

\Let $X_h = \bigtimes_{(r,x) \in E_h} \mathtt{0}_x^r(\co)$
\label{algorithm:csob:xh}


\If{$E_h \neq \emptyset$ and $X_h = \emptyset$}

\ReturnAlgorithmic $\bfalse$

\ElsIf{$X_h = \emptyset$}

\ReturnAlgorithmic \csobAlgorithm$(h, I_T, \emptyset)$
\label{algorithm:csob:call-csob-h}


\Else

\ForAll{$\vec{t}_\Delta \in X_h$}

\State $h' \gets h \bigoplus_{(r,x,t_x^r) \in \vec{t}_\Delta} \wro_x(t_x^r, r)$
\label{algorithm:csob:h-prime-definition}

\State $\mathtt{seen} \gets \emptyset$

\If{\csobAlgorithm$(h', I_T, \emptyset)$}
\label{algorithm:csob:call-csob-h-prime}

\ReturnAlgorithmic $\btrue$

\EndIf

\EndFor

\ReturnAlgorithmic $\bfalse$

\EndIf

\EndFunction
\Statex


\Function{\csobAlgorithm}{$\hist = \tup{T, \so, \wro}, I_T, T'$}
\label{algorithm:csob:call-csob}


\If{$|T'| = |T|$}
%\Comment{All transactions have been ordered.}
\label{algorithm:csob:base-case}

\ReturnAlgorithmic $\btrue$
\label{algorithm:csob:true-base-case}

\EndIf

\ForAll{$t \in T \setminus T'$ s.t. $T' \rhd_{(h, I_T)} (T' \cup \{t\})$}


\If{$(T' \cup \{t\}) \in \mathtt{seen}$}
\label{algorithm:csob:no-conflict-invalid}

\Continue

\ElsIf{\csobAlgorithm$(h, I_T, T' \cup \{t\})$}
\label{algorithm:csob:no-conflict-valid}

\ReturnAlgorithmic $\btrue$
\label{algorithm:csob:true-no-conflict}

\Else


\State $\mathtt{seen} \gets \mathtt{seen} \cup (T' \cup \{t\})$
\label{algorithm:csob:add-prefix-seen}

\EndIf

\EndFor

\ReturnAlgorithmic $\bfalse$

\EndFunction

\end{algorithmic}
     */

//$\mathtt{1}_x^r(\co) = \{ t \in T \ | \ (\trans{r}, t)\not\in \co \ \land \ \writeVar{t}{x} \ \land \ \where{r}(\valuewr[\overline{\wro}]{t}{x}) = 1\}$
// and $\mathtt{0}_x^r(\co) = \{ t \in T \ | \ (\trans{r}, t)\not\in \co \ \land \ \writeVar{t}{x} \ \land \ \where{r}(\valuewr[\overline{\wro}]{t}{x}) = 0\}$. Any witness $h' = \tup{T, \so, \wro'}$


//\Let $X_h = \bigtimes_{(r,x) \in E_h} \mathtt{0}_x^r(\co)$


    public boolean areWRRelated(Transaction t, ReadEvent r){
        var tr = transactions.get(r.getId()).get(r.getSo());
        var i = translator.get(t);
        var j = translator.get(tr);
        return wroReadTransactions.get(i).get(j).get(r.getPo());
    }

    public boolean areSORelated(Transaction t1, Transaction t2){
        var i = translator.get(t1);
        var j = translator.get(t2);
        return soTransactions.get(i).get(j);
    }


    public boolean areWRRelated(Transaction t1, Transaction t2){
        var i = translator.get(t1);
        var j = translator.get(t2);
        return wroTransactions.get(i).get(j);
    }

    public boolean areSOUWRRelated(Transaction t1, Transaction t2){
        return areSORelated(t1, t2) || areWRRelated(t1, t2);
    }

    public boolean areSOUWRRelated(Transaction t, ReadEvent r){
        return areSORelated(t, transactions.get(r.getId()).get(r.getSo())) || areWRRelated(t, r);
    }

    public ArrayList<ArrayList<Boolean>> getWroTransactions() {
        return wroTransactions;
    }

    public ArrayList<ArrayList<ArrayList<Boolean>>> getWroReadTransactions() {
        return wroReadTransactions;
    }

    public ArrayList<ArrayList<Boolean>> getSoTransactions() {
        return soTransactions;
    }

    public Map<String, Set<Variable>> getVariablePerTable() {
        return variablePerTable;
    }

    public Map<Variable, Set<Transaction>> getWritePerVariable() {
        return writePerVariable;
    }
}

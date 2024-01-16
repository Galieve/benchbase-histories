package com.oltpbenchmark.apiHistory;

import com.oltpbenchmark.apiHistory.events.*;
import com.oltpbenchmark.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class History {

    protected ArrayList<ArrayList<Transaction>> transactions;

    //Read -> Write
    protected Map<Variable,Map<ReadEvent, WriteEvent>> wro;

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
    }

    public int width() {
        return transactions.size();
    }

    public ArrayList<ArrayList<Transaction>> getTransactions() {
        return transactions;
    }

    public Map<Variable, Map<ReadEvent, WriteEvent>> getWro() {
        return wro;
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
        var events = transactions.get(0).get(0).getEvents();
        var initEvent = new InitialAbsentEvent(0,0,events.size(), writeSet);
        events.add(initEvent);
        writesIDs.put(new EventID(0,0,0), initEvent.getWriteEvent());
        transactions.get(0).get(0).resetWriteSet();
    }


    private void setWR(Variable variable, ReadEvent r, WriteEvent w) {
        wro.get(variable).put(r, w);
    }

    private ArrayList<ArrayList<Boolean>> computeCO(ArrayList<ArrayList<Boolean>> rInit){
        var co = HistoryUtil.deepclone(rInit);
        for(var ses: transactions){
            for(var t2 : ses){
                var ws = t2.getWriteSet();
                for(var p : ws.entrySet()) {
                    var variable = p.getKey();
                    for (var rw : wro.get(variable).entrySet()) {
                        var r = rw.getKey();
                        var w = rw.getValue();
                        var t3 = transactions.get(r.getId()).get(r.getSo());
                        var t1 = transactions.get(w.getId()).get(w.getSo());

                        int i = translator.get(t2);
                        int j = translator.get(t1);
                        if(i == j) continue;
                        if (co.get(i).get(j)) continue;
                        if (t3.satisfyConstraint(this, rInit, t2, r, variable)) {
                            co.get(i).set(j, true);
                        }
                    }
                }
            }
        }

        return co;

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

    protected boolean isOneSetEmpty(ArrayList<ArrayList<Boolean>> co, Variable x, ReadEvent r){
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

    protected Set<Transaction> getZeroSet(ArrayList<ArrayList<Boolean>> co, Variable x, ReadEvent r){
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



    //\Let $E_h = \{(r,x) \ | \ r \in \readOp{h}, x \in \Vars. \wro_x^{-1}(r) \ uparrow $ and $\mathtt{1}_r^x(\co) \neq \emptyset\}$
    protected Set<Pair<ReadEvent, Variable>> getEh(ArrayList<ArrayList<Boolean>> co){

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
                            if(!wro.get(variable).containsKey(r) && !isOneSetEmpty(co, variable, r)){
                                eh.add(new Pair<>(r, variable));
                            }
                        }
                    }

                }
            }
        }
        return eh;
    }


//\Let $X_h = \bigtimes_{(r,x) \in E_h} \mathtt{0}_x^r(\co)$

    /*
    protected List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> getXh(ArrayList<ArrayList<Boolean>> co, Set<Pair<ReadEvent, Variable>> eh) {
        var xh = new ArrayList<ArrayList<Pair<Variable,Pair<ReadEvent,Transaction>>>>();
        for (var p : eh) {
            var read = p.first;
            var variable = p.second;
            var zeroSet = getZeroSet(co, variable, read);
            var mappedZS = zeroSet.stream().map(t -> new Pair<>(variable, new Pair<>(read,t))).toList();
            xh.add(new ArrayList<>(mappedZS));
        }
        if(xh.size() == 0){
            return new ArrayList<>();
        }
        else return HistoryUtil.cartesianProduct(xh);
    }
     */

    protected List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> getXh(ArrayList<ArrayList<Boolean>> co, Set<Pair<ReadEvent, Variable>> eh) {
        var xh = new ArrayList<List<Pair<Variable,Pair<ReadEvent,Transaction>>>>();
        for (var p : eh) {
            var read = p.first;
            var variable = p.second;
            var zeroSet = getZeroSet(co, variable, read);
            var mappedZS = zeroSet.stream().map(t -> new Pair<>(variable, new Pair<>(read,t))).toList();
            if(mappedZS.size() == 0) return new ArrayList<>();
            xh.add(new ArrayList<>(mappedZS));
        }
        return xh;
    }


    //We assume xh.size() == index.size()
    protected boolean nextSet(List<List<Pair<Variable, Pair<ReadEvent, Transaction>>>> xh, ArrayList<Integer> index){

        for(int i = index.size() - 1; i >= 0; --i){
            index.set(i, index.get(i)+1);

            if(index.get(i) == xh.get(i).size()){
                index.set(i, 0);
            }
            else return true;

        }
        return false;
    }


    private ArrayList<ArrayList<Boolean>> computeCOFixpoint(ArrayList<ArrayList<Boolean>> coInit) throws InterruptedException {
        var co = coInit;
        int i = 0;
        do {
            coInit = co;
            co = computeCO(coInit);
            if(Thread.interrupted()) throw new InterruptedException();
            ++i;
        }
        while (!HistoryUtil.equals(co, coInit));
        LOG.debug("#Iterations COFixpoint: " + i);
        return co;
    }

    protected boolean dfsCOAcyclic(ArrayList<ArrayList<Boolean>> co, ArrayList<Integer> color, Integer u){
        color.set(u,1);
        for(int v = 0; v < co.get(u).size(); ++v){
            if(u == v) continue;
            if(co.get(u).get(v)
               && color.get(v) == 0){
                boolean b = dfsCOAcyclic(co, color, v);
                if(!b) return false;

            }
            else if(co.get(u).get(v)
                    && color.get(v) == 1){
                return false;
            }
        }
        color.set(u,2);
        return true;
    }

    protected boolean isCommitOrderAcyclic(ArrayList<ArrayList<Boolean>> co){
        //0 = not visited, 1 = in process, 2 = finished
        int n = co.size();
        ArrayList<Integer> color = new ArrayList<>(Collections.nCopies(n,0));
        for(int i = 0; i < n; ++i){
            if(color.get(i) == 0){
                if(!dfsCOAcyclic(co, color, i)){
                    return false;
                }
            }
        }
        return true;
    }

    public boolean checkSOBound(ResultHistory result) throws InterruptedException {
        long now = System.nanoTime();
        var soUwr = computeSoUWr();
        var coInit = HistoryUtil.transitiveClosure(soUwr);

        var co = computeCOFixpoint(coInit);
        if(Thread.interrupted()) throw new InterruptedException();
        if(!isCommitOrderAcyclic(co)){
            LOG.debug("CO is NOT acyclic!");
            return false;
        }
        LOG.debug("CO is acyclic");

        var eh = getEh(co);
        if(Thread.interrupted()) throw new InterruptedException();
        var xh = getXh(co, eh);
        if(Thread.interrupted()) throw new InterruptedException();



        if(eh.size() != 0 && xh.size() == 0){
            LOG.debug("No possible extension!");
            return false;
        }
        else if(xh.size() == 0){
            LOG.debug("Unique CSOB call");
            return csob(co, new PrefixHistory(this), new HashSet<>());
        }
        else{
            var index = new ArrayList<>(Collections.nCopies(xh.size(),0));
            int numCsob = 0;
            LOG.debug("Multiple CSOB calls");
            do {
                var h = new History(this);
                var coH = HistoryUtil.deepclone(co);
                LOG.debug("Call #" + numCsob);
                for (int i = 0; i < index.size(); ++i) {
                    var zeroSet = xh.get(i);
                    var p = zeroSet.get(index.get(i));
                    var variable = p.first;
                    var r = p.second.first;
                    var w = p.second.second.getWriteSet().get(variable).first;
                    h.setWR(variable, r, w);

                    int j = translator.get(transactions.get(w.getId()).get(w.getSo()));
                    int k = translator.get(transactions.get(r.getId()).get(r.getSo()));
                    if(j != k)
                        coH.get(j).set(k, true);
                }
                coH = computeCOFixpoint(coH);
                if(isCommitOrderAcyclic(coH)){
                    if(h.csob(coH, new PrefixHistory(this), new HashSet<>())) {
                        LOG.debug("Call #" + numCsob + ": consistent!");
                       return true;
                   }
                }
                else{
                    LOG.debug("Call #" + numCsob + ": CO is NOT acyclic!");
                }
                ++numCsob;

            }while(!Thread.interrupted() && nextSet(xh, index));
            if(Thread.interrupted()) throw new InterruptedException();
            return false;
        }
    }

    protected ArrayList<ArrayList<Boolean>> computeSoUWr() {
        var soUwr = HistoryUtil.deepclone(wroTransactions);
        for(int i = 0; i < soUwr.size(); ++i){
            for(int j = 0; j < soUwr.get(i).size(); ++j){
                soUwr.get(i).set(j, soUwr.get(i).get(j) || soTransactions.get(i).get(j));
            }
        }
        return soUwr;
    }

    private boolean csob(ArrayList<ArrayList<Boolean>> co, PrefixHistory prefix, Set<PrefixHistory> seen) throws InterruptedException {
        if(prefix.size() == co.size()){
            return true;
        }
        for(var ses: transactions){
            for(var t: ses){
                if(Thread.interrupted()) throw new InterruptedException();
                if(prefix.contains(t)) continue;
                if(!prefix.isConsistent(co, t)) continue;
                var ext = prefix.extend(t);
                if(seen.contains(ext)) continue;
                if(csob(co, ext, seen)) return true;
                seen.add(ext);
            }
        }
        return false;
    }

    public boolean areSORelated(Transaction t1, Transaction t2){
        var i = translator.get(t1);
        var j = translator.get(t2);
        return soTransactions.get(i).get(j);
    }

    public boolean areWRRelated(Transaction t, ReadEvent r){
        var tr = transactions.get(r.getId()).get(r.getSo());
        var i = translator.get(t);
        var j = translator.get(tr);
        return wroReadTransactions.get(i).get(j).get(r.getPo());
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

}

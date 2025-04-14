package com.oltpbenchmark.historyModelHistory;

import com.oltpbenchmark.Phase;
import com.oltpbenchmark.SubmittedProcedure;
import com.oltpbenchmark.WorkloadState;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Procedure;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.historyModelHistory.events.AbortEvent;
import com.oltpbenchmark.historyModelHistory.events.Event;
import com.oltpbenchmark.historyModelHistory.events.Transaction;
import com.oltpbenchmark.historyModelHistory.isolationLevels.IsolationLevel;
import com.oltpbenchmark.types.DatabaseType;
import com.oltpbenchmark.types.State;
import com.oltpbenchmark.types.TransactionStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;

public abstract class WorkerHistory<T extends BenchmarkModule> extends Worker<T> {

    protected final ArrayList<Transaction> transactions;

    private static final Logger LOG = LoggerFactory.getLogger(WorkerHistory.class);

    private static final Logger ABORT_LOG = LoggerFactory.getLogger("com.oltpbenchmark.api.ABORT_LOG");



    public WorkerHistory(T benchmark, int id) {
        super(benchmark, id);
        transactions = new ArrayList<>();
    }
    @Override
    protected TransactionType getTransactionType(SubmittedProcedure pieceOfWork, Phase phase, State state, WorkloadState workloadState) {
        var tType = super.getTransactionType(pieceOfWork, phase, state, workloadState);
        var tTypeCast = (TransactionTypeHistory) tType;
        tTypeCast.setStartTime(pieceOfWork.getStartTime());
        return tTypeCast;
    }

    protected abstract TransactionStatus executeWorkHistory(Connection conn, TransactionType txnType, ArrayList<Event> events, int id, int soID) throws Procedure.UserAbortException, SQLException;

    protected final TransactionStatus executeWork(Connection conn, TransactionType txnType) throws Procedure.UserAbortException, SQLException{
        return executeWorkHistory(conn, txnType, new ArrayList<>(), getId(), 0);
    }



    @Override
    protected void initialize() {

    }

    public ArrayList<Transaction> getTransactions() {
        return transactions;
    }

    /**
     * Called in a loop in the thread to exercise the system under test. Each
     * implementing worker should return the TransactionTypeHistory handle that was
     * executed.
     *
     * @param databaseType TODO
     * @param transactionType TODO
     */
    protected void doWork(DatabaseType databaseType, TransactionType transactionType){

        try {
            int retryCount = 0;
            int maxRetryCount = configuration.getMaxRetries();
            boolean done = false;

            //retryCount < maxRetryCount && this.workloadState.getGlobalState() != State.DONE
            while (retryCount < maxRetryCount && !done) {
                TransactionStatus status = TransactionStatus.UNKNOWN;
                if (this.conn == null) {
                    try {
                        this.conn = this.benchmark.makeConnection();
                        this.conn.setAutoCommit(false);
                        this.conn.setTransactionIsolation(this.configuration.getIsolationMode());
                    } catch (SQLException ex) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("%s failed to open a connection...", this));
                        }
                        retryCount++;
                        continue;
                    }
                }

                var connIso = conn.getTransactionIsolation();
                ArrayList<Event> events = new ArrayList<>();


                //We prioritize the isolation level declared by the method rather the global isolation level.
                if(transactionType instanceof TransactionTypeHistory){
                    var typeH = (TransactionTypeHistory) transactionType;
                    if(typeH.getIsolationLevel() != null) {
                        conn.setTransactionIsolation(typeH.getIsolationLevel().getMode());
                    }
                }

                var iso = IsolationLevel.get(conn.getTransactionIsolation());

                try {

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s attempting...", this, transactionType));
                    }

                    status = executeWorkHistory(conn, transactionType, events, getId(), transactions.size());

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s completed with status [%s]...", this, transactionType, status.name()));
                    }

                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format("%s %s committing...", this, transactionType));
                    }

                    conn.commit();
                    if(this.workloadState.getGlobalState() != State.DONE) {
                        transactions.add(new Transaction(events, this.getId(), transactions.size(), iso, transactionType.getName()));
                    }

                    conn.setTransactionIsolation(connIso);
                    done = true;
                    break;

                } catch (Procedure.UserAbortException ex) {
                    conn.rollback();
                    events.add(new AbortEvent(this.getId(), transactions.size(), events.size()));
                    transactions.add(new Transaction(events, this.getId(), transactions.size(), iso, transactionType.getName()));

                    conn.setTransactionIsolation(connIso);


                    ABORT_LOG.debug(String.format("%s Aborted", transactionType), ex);

                    status = TransactionStatus.USER_ABORTED;

                    break;

                } catch (SQLException ex) {
                    conn.rollback();
                    //events.add(new AbortEvent(this.getId(), transactions.size(), events.size()));
                    conn.setTransactionIsolation(connIso);

                    if (isRetryable(ex)) {
                        LOG.debug(String.format("Retryable SQLException occurred during [%s]... current retry attempt [%d], max retry attempts [%d], sql state [%s], error code [%d].", transactionType, retryCount, maxRetryCount, ex.getSQLState(), ex.getErrorCode()), ex);

                        status = TransactionStatus.RETRY;

                        retryCount++;
                    } else {
                        LOG.warn(String.format("SQLException occurred during [%s] and will not be retried... sql state [%s], error code [%d].", transactionType, ex.getSQLState(), ex.getErrorCode()), ex);

                        status = TransactionStatus.ERROR;

                        break;
                    }

                } finally {

                    if (this.configuration.getNewConnectionPerTxn() && this.conn != null) {
                        try {
                            this.conn.close();
                            this.conn = null;
                        } catch (SQLException e) {
                            LOG.error("Connection couldn't be closed.", e);
                        }
                    }

                    updateHistogram(status, transactionType);

                }

            }
        } catch (SQLException ex) {
            String msg = String.format("Unexpected SQLException in '%s' when executing '%s' on [%s]", this, transactionType, databaseType.name());

            throw new RuntimeException(msg, ex);
        }

    }
}
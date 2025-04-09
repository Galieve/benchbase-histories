package com.oltpbenchmark.apiHistory;

public class ResultHistory {

    private Long evalStartTime;

    private Long evalEndTime;

    private Boolean consistent;

    private boolean timeout;
    private Long createStartTime;

    private Long createEndTime;
    private History history;


    public ResultHistory(){
        consistent = null;
        timeout = false;
    }


    public Boolean getConsistent() {
        return consistent;
    }

    public void setConsistent(Boolean consistent) {
        this.consistent = consistent;
    }

    public boolean isTimeout() {
        return timeout;
    }

    public void setTimeout(boolean timeout) {
        this.timeout = timeout;
    }

    public Long getEvalStartTime() {
        return evalStartTime;
    }

    public void setEvalStartTime(long evalStartTime) {
        this.evalStartTime = evalStartTime;
    }

    public Long getEvalEndTime() {
        return evalEndTime;
    }

    public void setEvalEndTime(long evalEndTime) {
        this.evalEndTime = evalEndTime;
    }

    public Long getCreateStartTime() {
        return createStartTime;
    }

    public void setCreateStartTime(long createStartTime) {
        this.createStartTime = createStartTime;
    }

    public Long getCreateEndTime() {
        return createEndTime;
    }

    public void setCreateEndTime(long createEndTime) {
        this.createEndTime = createEndTime;
    }

    public void setHistory(History h) {
        this.history = h;
    }

    public History getHistory() {
        return history;
    }
}

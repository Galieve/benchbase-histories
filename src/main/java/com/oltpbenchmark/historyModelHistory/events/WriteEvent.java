package com.oltpbenchmark.historyModelHistory.events;

import java.util.HashMap;

public class WriteEvent{

    protected Event parent;
    protected HashMap<Variable, Value> writeSet;

    public WriteEvent(Event parent, HashMap<Variable, Value> writeSet) {
        this.parent = parent;
        this.writeSet = writeSet;
    }

    public boolean writes(Variable variable){
        return writeSet.containsKey(variable);
    }
    //If isWritten(v) == true but getWrittenValue(v) == null, the variable is deleted.
    public Value getWrittenValue(Variable variable) {
        return writeSet.get(variable);
    }

    public HashMap<Variable, Value> getWriteSet(){
        return writeSet;
    }

    public String toString(){
        var sb = new StringBuilder();
        sb.append("WriteSet:\n");
        boolean first = true;
        for(var wro : getWriteSet().entrySet()){
            if(!first) sb.append("\n");
            else first = false;
            sb.append("\t").append(wro.getKey()).append(": ").append(wro.getValue());
        }
        return sb.toString();
    }

    public final Integer getId() {
        return parent.getId();
    }

    public final Integer getSo() {
        return parent.getSo();
    }

    public final EventID getEventID() {return parent.getEventID();}
    public final int getPo() {
        return parent.getPo();
    }


}

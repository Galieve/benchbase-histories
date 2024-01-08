package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;

public interface WriteEvent extends EventInterface{

    public boolean writes(Variable variable);
    public Value getWrittenValue(Variable variable);
    //public HashMap<String, >


    @Override
    default boolean isWrite() {
        return true;
    }

    public HashMap<Variable, Value> getWriteSet();
}

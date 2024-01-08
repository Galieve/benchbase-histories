package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;

public interface ReadEvent extends EventInterface{

    public boolean satisfyWhere(Value value);

    public EventID readsVariable(Variable variable);

    @Override
    default boolean isRead(){
        return true;
    }

    public HashMap<Variable, EventID> getWroInverse();


}

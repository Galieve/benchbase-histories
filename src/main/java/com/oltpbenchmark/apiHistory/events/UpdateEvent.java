package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

public class UpdateEvent extends Event implements ReadEvent, WriteEvent {

    protected HashMap<Variable, Value> writeSet;
    protected HashMap<Variable, EventID> wroInverse;
    protected Function<Value, Boolean> wherePredicate;

    public UpdateEvent(Integer id, Integer so, Integer po, HashMap<Variable, Value> writeSet, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate, Set<String> tableNames) {
        super(EventType.UPDATE, id, so, po, tableNames);
        this.writeSet = writeSet;
        this.wroInverse = wroInverse;
        this.wherePredicate = wherePredicate;
    }

    @Override
    public boolean satisfyWhere(Value value) {
        return wherePredicate.apply(value);
    }

    @Override
    public EventID readsVariable(Variable variable) {
        return wroInverse.get(variable);
    }

    @Override
    public boolean writes(Variable variable) {
        return writeSet.containsKey(variable);
    }

    @Override
    //If isWritten(v) == true but getWrittenValue(v) == null, the variable is deleted.
    public Value getWrittenValue(Variable variable) {
        return writeSet.get(variable);
    }

    @Override
    public HashMap<Variable, Value> getWriteSet() {
        return writeSet;
    }

    @Override
    public HashMap<Variable, EventID> getWroInverse() {
        return wroInverse;
    }
}

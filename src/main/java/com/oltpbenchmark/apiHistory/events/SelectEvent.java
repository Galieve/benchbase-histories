package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.Set;
import java.util.function.Function;

public class SelectEvent extends Event implements ReadEvent {

    protected HashMap<Variable, EventID> wroInverse;
    protected Function<Value, Boolean> wherePredicate;

    public SelectEvent(Integer id, Integer so, Integer po, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate, Set<String> tableNames) {
        super(EventType.SELECT, id, so, po, tableNames);
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
    public boolean isWrite() {
        return false;
    }

    @Override
    public HashMap<Variable, EventID> getWroInverse() {
        return wroInverse;
    }
}

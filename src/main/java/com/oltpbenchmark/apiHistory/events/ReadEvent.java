package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;
import java.util.function.Function;

public class ReadEvent{


    protected Event parent;
    protected HashMap<Variable, EventID> wroInverse;
    protected Function<Value, Boolean> wherePredicate;

    public ReadEvent(Event parent, HashMap<Variable, EventID> wroInverse, Function<Value, Boolean> wherePredicate) {
        this.wroInverse = wroInverse;
        this.wherePredicate = wherePredicate;
        this.parent = parent;
    }

    public boolean satisfyWhere(Value value){
        return wherePredicate.apply(value);
    }

    public EventID readsVariable(Variable variable){
        return wroInverse.get(variable);
    }

    public HashMap<Variable, EventID> getWroInverse(){
        return wroInverse;
    }

    public String toString(){
        var sb = new StringBuilder();
        sb.append("WR:\n");
        boolean first = true;
        for(var wro : getWroInverse().entrySet()){
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

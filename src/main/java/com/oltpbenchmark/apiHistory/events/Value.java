package com.oltpbenchmark.apiHistory.events;

import java.util.HashMap;

public class Value {

    protected Variable variable;
    protected HashMap<String, String> fieldNameToValue;

    public Value(Variable variable, HashMap<String, String> fieldNameToValue) {
        this.variable = variable;
        this.fieldNameToValue = fieldNameToValue;
    }

    public Value(Value val) {
        variable = val.variable;
        fieldNameToValue = val.fieldNameToValue;
    }

    public Variable getVariable() {
        return variable;
    }

    public String getValue(String fieldName){
        return fieldNameToValue.get(fieldName.toUpperCase());
    }

    public void setValue(String fieldName, String value){
        fieldNameToValue.put(fieldName, value);
    }

    @Override
    public String toString() {
        return "Value{" +
               "variable=" + variable +
               ", fieldNameToValue=" + fieldNameToValue +
               '}';
    }
}

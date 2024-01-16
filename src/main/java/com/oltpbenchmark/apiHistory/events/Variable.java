package com.oltpbenchmark.apiHistory.events;

import java.util.Objects;

public class Variable {

    protected String variable;

    public Variable(String variable) {
        this.variable = variable;
    }

    public String getStringName() {
        return variable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Variable variable1 = (Variable) o;
        return Objects.equals(variable, variable1.variable);
    }

    @Override
    public int hashCode() {
        return Objects.hash(variable);
    }

    @Override
    public String toString() {
        return "Variable{" +
               "variable='" + variable + '\'' +
               '}';
    }
}

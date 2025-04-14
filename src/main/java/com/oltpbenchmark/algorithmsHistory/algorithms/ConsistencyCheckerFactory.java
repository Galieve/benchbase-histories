package com.oltpbenchmark.algorithmsHistory.algorithms;

public class ConsistencyCheckerFactory {

    public static ConsistencyChecker getChecker(String name){

        switch (name){
            case "CSOB" : return new CheckSOBound();
            case "Naive" : return new NaiveCheckClientConsistency();
            default: return null;
        }
    }
}

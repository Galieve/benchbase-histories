package com.oltpbenchmark.apiHistory.util;

import com.oltpbenchmark.apiHistory.events.ReadEvent;
import com.oltpbenchmark.apiHistory.events.Transaction;
import com.oltpbenchmark.apiHistory.events.Variable;
import com.oltpbenchmark.apiHistory.events.WriteEvent;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HistoryUtil {


    public static <T> ArrayList<ArrayList<T>> deepclone(ArrayList<ArrayList<T>> list){
        var copy = new ArrayList<ArrayList<T>>();
        for(int i = 0; i < list.size(); ++i){
            copy.add(new ArrayList<>());
            for(var t : list.get(i)){
                copy.get(i).add(t);
            }
        }
        return copy;
    }
    public static ArrayList<ArrayList<Boolean>> transitiveClosure(ArrayList<ArrayList<Boolean>> relation){
        var trRel = deepclone(relation);
        for(int k = 0; k < trRel.size(); ++k){
            for(int i = 0; i < trRel.size(); ++i){
                for(int j = 0; j < trRel.size(); ++j){
                    if(trRel.get(i).get(k) && trRel.get(k).get(j)){
                        trRel.get(i).set(j, true);
                    }
                }
            }
        }
        return trRel;
    }

    public static<T> boolean equals(ArrayList<ArrayList<T>> a, ArrayList<ArrayList<T>> b){
        if(a.size() != b.size()) return false;
        for(int i = 0; i < a.size(); ++i){
            if(a.get(i).size() != b.get(i).size()) return false;
            for(int j = 0; j < a.get(i).size(); ++j){
                if(!a.get(i).get(j).equals(b.get(i).get(j))){
                    return false;
                }
            }
        }
        return true;
    }

    public static <T, U> boolean equals(Map<T, U> a, Map<T, U> b) {
        if(a.size() != b.size()) return false;
        for(var ea : a.entrySet()){
            if(!b.containsKey(ea.getKey())) return false;
            if(!b.get(ea.getKey()).equals(ea.getValue())) return false;
        }
        return true;
    }


    public static <T> List<List<T>> cartesianProduct(ArrayList<ArrayList<T>> sets) {
        return cartesianProduct(sets,0).peek(Collections::reverse).collect(Collectors.toList());
    }

    private static <T> Stream<List<T>> cartesianProduct(ArrayList<ArrayList<T>> sets, int index) {
        if (index == sets.size()) {
            List<T> emptyList = new ArrayList<>();
            return Stream.of(emptyList);
        }
        List<T> currentSet = sets.get(index);
        return currentSet.stream().flatMap(element -> cartesianProduct(sets, index+1)
            .map(list -> {
                List<T> newList = new ArrayList<>(list);
                newList.add( element);
                return newList;
            }));
    }

    public static <T,U,V > Map<T, Map<U, V>> deepclone(Map<T, Map<U, V>> map) {
        var copy = new HashMap<T, Map<U, V>>();
        for (var entry : map.entrySet()) {
            var copiedMap = new HashMap<>(entry.getValue());
            copy.put(entry.getKey(), copiedMap);
        }
        return copy;
    }


    public static <T,U> Map<T,Set<U>> deepcloneSet(Map<T, Set<U>> map) {
        var copy = new HashMap<T, Set<U>>();
        for (var entry : map.entrySet()) {
            var copiedSet = new HashSet<>(entry.getValue());
            copy.put(entry.getKey(), copiedSet);
        }
        return copy;
    }


}

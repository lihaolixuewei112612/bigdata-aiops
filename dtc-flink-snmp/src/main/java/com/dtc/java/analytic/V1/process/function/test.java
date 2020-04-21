package com.dtc.java.analytic.V1.process.function;

import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2020-02-06
 *
 * @author :hao.li
 */
public class test {
static Map<String,String> result = new HashMap<>();
    public static void main(String[] args) {
        Map<String, String> mapA = new HashMap<>();
        Map<String, String> mapB = new HashMap<>();
        mapA.put("1", "2");
        mapA.put("2", "3");
        mapA.put("3", "4");
        mapB.put("1", "3");
        mapB.put("2", "4");
        mapB.put("3", "5");
        if (mapA.size() == mapB.size()) {
            for (String keyA : mapA.keySet()) {
                for (String keyB : mapB.keySet()){
                    if(keyA.equals(keyB)){
                        double valueA = Double.parseDouble(mapA.get(keyA));
                        double valueB = Double.parseDouble(mapB.get(keyB));
                        result.put(keyA, String.valueOf(valueA*valueB));
                    }
                }
            }
        }
        for(Map.Entry<String,String> str : result.entrySet()){
            System.out.println("key is: "+str.getKey()+","+"value is: "+str.getValue());
        }
    }
}

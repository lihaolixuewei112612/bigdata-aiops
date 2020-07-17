package com.dtc.java.analytic.V2.alarm;

import java.util.*;

/**
 * @Author : lihao
 * Created on : 2020-07-17
 * @Description : TODO描述类作用
 */
public class demo {
    public static void main(String[] args) {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        map.put(1, 8);
        map.put(3, 12);
        map.put(5, 53);
        map.put(123, 33);
        map.put(42, 11);
        map.put(44, 42);
        map.put(15, 3);
        System.out.println(getMinKey(map));
        System.out.println(getMinValue(map));
    }
    public static Object getMinKey(Map<Integer, Integer> map) {
        if (map == null) return null;
        Set<Integer> set = map.keySet();
        Object[] obj = set.toArray();
        Arrays.sort(obj);
        return obj[0];
    }


    public static Object getMinValue(Map<Integer, Integer> map) {
        if (map == null) return null;
        Collection<Integer> c = map.values();
        Object[] obj = c.toArray();
        Arrays.sort(obj);
        return obj[0];
    }
}

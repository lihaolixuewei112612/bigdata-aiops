package com.dtc.java.analytic.V1.common.utils;

import org.apache.directory.api.util.Strings;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created on 2019-08-21
 * statistic the data of recive and error data
 *
 * @author :hao.li
 */
public class Count {
    Map<String, AtomicLong> counterMap = new HashMap<>();
    AtomicLong atomicLong = new AtomicLong(0);

    public Long increment(String counts) {
        AtomicLong atomicLong = counterMap.get(counts);
        if (atomicLong == null) {
            counterMap.put(counts, this.atomicLong);
            return 0L;
        } else {
            return counterMap.get(counts).incrementAndGet();
        }
    }

    public Long getCount(String counts) {
        return counterMap.get(counts).get();
    }
}

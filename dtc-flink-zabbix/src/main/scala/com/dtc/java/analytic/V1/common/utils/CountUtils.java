package com.dtc.java.analytic.V1.common.utils;

/**
 * Created on 2019-08-21
 *
 * @author :hao.li
 */
public class CountUtils {
    private static final String COUNTER_EVENTS_RECEIVED = "counter.events.received";
    private static final String COUNTER_EVENTS_RECEIVED_FAILURE = "counter.events.received.failure";
    private static final String COUNTER_EVENTS_ERROR = "counter.events.error";
    private static final String COUNTER_EVENTS_RIGHT = "counter.events.right";

    private static Count count = new Count();

    public static Long incrementEventReceivedCount() {
        return count.increment(COUNTER_EVENTS_RECEIVED);
    }

    public static Long incrementEventReceivedCount_Failuer() {
        return count.increment(COUNTER_EVENTS_RECEIVED_FAILURE);
    }

    public static Long incrementEventRightCount(String str) {
        return count.increment(str + "." + COUNTER_EVENTS_RIGHT);
    }

    public static Long incrementEventErrorCount(String str) {
        return count.increment(str + "." + COUNTER_EVENTS_ERROR);
    }

    public static Long getEventReceivedCount() {
        return count.getCount(COUNTER_EVENTS_RECEIVED);
    }

    public static Long getEventRightCount(String str) {
        return count.getCount(str + "." + COUNTER_EVENTS_RIGHT);
    }

    public static Long getEventErrorCount(String str) {
        return count.getCount(str + "." + COUNTER_EVENTS_ERROR);
    }
}

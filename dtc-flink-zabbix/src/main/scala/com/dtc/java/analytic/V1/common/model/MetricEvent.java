package com.dtc.java.analytic.V1.common.model;

/**
 * Created on 2019-10-17
 *
 * @author :hao.li
 */
public class MetricEvent {
    private String name;
    private String host;
    private String ip;
    private long ns;
    private String english;
    private long id;
    private long clock;
    private String oid;
    private double value;

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public long getNs() {
        return ns;
    }

    public String getEnglish() {
        return english;
    }

    public long getId() {
        return id;
    }

    public long getClock() {
        return clock;
    }

    public String getOid() {
        return oid;
    }

    public double getValue() {
        return value;
    }
}

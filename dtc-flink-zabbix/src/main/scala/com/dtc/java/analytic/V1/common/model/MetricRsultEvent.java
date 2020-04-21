package com.dtc.java.analytic.V1.common.model;

/**
 * Created on 2019-10-17
 *
 * @author :hao.li
 */
public class MetricRsultEvent {
    private String ip;
    private String host;
    private String oid;
    private String name;
    private String english;
    private long clock;
    private double value;

    public MetricRsultEvent(String ip, String host, String oid, String name, String english, long clock, double value) {
        this.ip = ip;
        this.host = host;
        this.oid = oid;
        this.name = name;
        this.english = english;
        this.clock = clock;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getIp() {
        return ip;
    }

    public String getEnglish() {
        return english;
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

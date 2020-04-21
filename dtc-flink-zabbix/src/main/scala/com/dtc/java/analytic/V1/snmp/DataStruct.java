package com.dtc.java.analytic.V1.snmp;

/**
 * Created on 2019-08-12
 *
 * @author :hao.li
 */
public class DataStruct {
    public String System_name;
    public String Host;

    public String getSystem_name() {
        return System_name;
    }

    public void setSystem_name(String system_name) {
        System_name = system_name;
    }

    public String getHost() {
        return Host;
    }

    public void setHost(String host) {
        Host = host;
    }

    public String getZB_Name() {
        return ZB_Name;
    }

    public void setZB_Name(String ZB_Name) {
        this.ZB_Name = ZB_Name;
    }

    public String getZB_Code() {
        return ZB_Code;
    }

    public void setZB_Code(String ZB_Code) {
        this.ZB_Code = ZB_Code;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public Double getValue() {
        return value;
    }

    public void setValue(Double value) {
        this.value = value;
    }

    public String ZB_Name;
    public String ZB_Code;
    public String time;
    public Double value;

    public DataStruct(String system_name, String host, String ZB_Name, String ZB_Code, String time, Double value) {
        this.System_name = system_name;
        this.Host = host;
        this.ZB_Name = ZB_Name;
        this.ZB_Code = ZB_Code;
        this.time = time;
        this.value = value;
    }
}

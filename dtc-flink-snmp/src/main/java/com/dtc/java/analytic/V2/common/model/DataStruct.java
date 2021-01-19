package com.dtc.java.analytic.V2.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
//@Data
public class DataStruct {
    private String System_name;
    private String Host;
    private String zbFourName;
    private String zbLastCode;
    private String nameCN;
    private String nameEN;
    private String time;
    public String value;

    public DataStruct() {
    }



    public DataStruct(String system_name, String host, String zbFourName, String zbLastCode, String nameCN, String nameEN, String time, String value) {
        System_name = system_name;
        Host = host;
        this.zbFourName = zbFourName;
        this.zbLastCode = zbLastCode;
        this.nameCN = nameCN;
        this.nameEN = nameEN;
        this.time = time;
        this.value = value;
    }

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

    public String getZbFourName() {
        return zbFourName;
    }

    public void setZbFourName(String zbFourName) {
        this.zbFourName = zbFourName;
    }

    public String getZbLastCode() {
        return zbLastCode;
    }

    public void setZbLastCode(String zbLastCode) {
        this.zbLastCode = zbLastCode;
    }

    public String getNameCN() {
        return nameCN;
    }

    public void setNameCN(String nameCN) {
        this.nameCN = nameCN;
    }

    public String getNameEN() {
        return nameEN;
    }

    public void setNameEN(String nameEN) {
        this.nameEN = nameEN;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

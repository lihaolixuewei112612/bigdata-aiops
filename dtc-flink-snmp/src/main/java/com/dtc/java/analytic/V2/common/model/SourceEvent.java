package com.dtc.java.analytic.V2.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-14
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
public class SourceEvent {
    private String time;
    private String code;
    private String host;
    private String nameCN;
    private String nameEN;
    private String value;


    public SourceEvent(String time, String code, String host, String nameCN, String nameEN, String value) {
        this.time = time;
        this.code = code;
        this.host = host;
        this.nameCN = nameCN;
        this.nameEN = nameEN;
        this.value = value;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
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

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}

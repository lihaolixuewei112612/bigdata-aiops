package com.dtc.java.analytic.V2.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
public class AlterStruct {
    //系统类型///
    private String System_name;
    ///ip
    private String Host;
    //前五位编码
    private String zbFourName;
    //最后一位小数点编码
    private String zbLastCode;
    //中文名
    private String nameCN;
    //英文名
    private String nameEN;
    //event time
    private String event_time;
    //system_time
    private String system_time;
    //具体告警值
    private String value;
    //告警等级
    private String level;
    //资产唯一值，asseti_id
    private String unique_id;
    //阈值
    private String yuzhi;
    //ip-code
    private String gaojing;

    public AlterStruct(String system_name, String host, String zbFourName, String zbLastCode, String nameCN, String nameEN, String event_time, String system_time, String value, String level, String unique_id, String yuzhi, String gaojing) {
        System_name = system_name;
        Host = host;
        this.zbFourName = zbFourName;
        this.zbLastCode = zbLastCode;
        this.nameCN = nameCN;
        this.nameEN = nameEN;
        this.event_time = event_time;
        this.system_time = system_time;
        this.value = value;
        this.level = level;
        this.unique_id = unique_id;
        this.yuzhi = yuzhi;
        this.gaojing = gaojing;
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

    public String getEvent_time() {
        return event_time;
    }

    public void setEvent_time(String event_time) {
        this.event_time = event_time;
    }

    public String getSystem_time() {
        return system_time;
    }

    public void setSystem_time(String system_time) {
        this.system_time = system_time;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getUnique_id() {
        return unique_id;
    }

    public void setUnique_id(String unique_id) {
        this.unique_id = unique_id;
    }

    public String getYuzhi() {
        return yuzhi;
    }

    public void setYuzhi(String yuzhi) {
        this.yuzhi = yuzhi;
    }

    public String getGaojing() {
        return gaojing;
    }

    public void setGaojing(String gaojing) {
        this.gaojing = gaojing;
    }
}

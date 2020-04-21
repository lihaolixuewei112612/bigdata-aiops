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
@AllArgsConstructor
public class AlterStruct {
    //系统类型
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
}

package com.dtc.java.analytic.V1.common.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Created on 2019-12-23
 *
 * @author :hao.li
 */
public class ResultMetricEvent {
    //systemName格式如下：101_101+<ip>
    private String systemName;
    //indicatorsName:指标名编码，为系统编码的前四位
    private String indicatorsName;
    //indicatorsCode:指标名称，为系统编码的第五位
    private String indicatorsCode;
    private String time;
    private String value;
    private String host;

    public ResultMetricEvent(String systemName, String indicatorsName, String indicatorsCode, String time, String value, String host) {
        this.systemName = systemName;
        this.indicatorsName = indicatorsName;
        this.indicatorsCode = indicatorsCode;
        this.time = time;
        this.value = value;
        this.host = host;
    }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public String getIndicatorsName() {
        return indicatorsName;
    }

    public void setIndicatorsName(String indicatorsName) {
        this.indicatorsName = indicatorsName;
    }

    public String getIndicatorsCode() {
        return indicatorsCode;
    }

    public void setIndicatorsCode(String indicatorsCode) {
        this.indicatorsCode = indicatorsCode;
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

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }


}

class MyMapFunction implements MapFunction<String, Tuple5<Tuple2<String, String>, String, String, String, String >> {

    //对json数据进行解析并且存入Tuple
    @Override
    public Tuple5<Tuple2<String, String>, String, String, String, String > map(String s) throws Exception {
        Tuple5<Tuple2<String, String>, String, String, String, String > abc = null;
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(s);
        String[] codes = json.get("code").textValue().split("_");
        String system_name = codes[0].trim()+"_"+codes[1].trim() ;
        String ZB_name = codes[0].trim()+"_"+codes[1].trim() + "_" + codes[2].trim() + "_" + codes[3].trim();
        String ZB_code = codes[4].trim();
        String time = json.get("time").textValue();
        String value = json.get("value").textValue();
        String host = json.get("host").textValue().trim();
        abc = Tuple5.of(Tuple2.of(system_name, host), ZB_name, ZB_code, time, value);
        //System.out.println(ZB_name+"@@@"+host+"@@@"+time+"@@@"+value);
        return abc;

    }
}

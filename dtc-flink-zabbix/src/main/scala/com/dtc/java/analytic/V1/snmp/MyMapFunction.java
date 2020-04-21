package com.dtc.java.analytic.V1.snmp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class MyMapFunction implements MapFunction<String, Tuple4<Tuple3<String, String, String>, String, String, Double>> {

    @Override
    public Tuple4<Tuple3<String, String, String>, String, String, Double> map(String s) throws Exception {
        Tuple4<Tuple3<String, String, String>, String, String, Double> abc = null;
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode json = objectMapper.readTree(s);
        String[] codes = json.get("code").textValue().split("\\|");
        String system_name = codes[0].trim() + "_" + codes[1].trim();
        String ZB_name = system_name + "_" + codes[2].trim() + "_" + codes[3].trim();
        String ZB_code = codes[4].trim();
        String time = json.get("time").textValue();
        Double value = json.get("value").asDouble();
        String host = json.get("host").textValue().trim();
        abc = Tuple4.of(Tuple3.of(system_name, host, ZB_name), ZB_code, time, value);
        return abc;
    }
}

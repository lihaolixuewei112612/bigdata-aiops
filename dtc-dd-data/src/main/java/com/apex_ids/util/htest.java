package com.apex_ids.util;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class htest {
    public static void main(String[] args) {
        String zsbd = "[{'start': 20200203, 'end': 20200305, 'value': 4014.42, 'type': 0}, {'start': 20200306, 'end': 20200522, 'value': 3809.49, 'type': -1}, {'start': 20200525, 'end': 20200709, 'value': 3998.6, 'type': 1}, {'start': 20200710, 'end': 20200731, 'value': 4624.31, 'type': 0}]";
        String pattern = "(\\{.*?\\})";
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(zsbd);
        List<Map<String, String>> data = new ArrayList<>();
        while (matcher.find()){
            data.add(stringToMap(matcher.group().replaceAll(" |'|\\{|\\}", "")));
        }
        for (Map<String, String> d: data){
            System.out.println(d);
        }
    }
    private static Map<String, String> stringToMap(String s){
        String[] strings = s.split(",");
        Map<String, String> map = new HashMap<>();
        for (String string: strings){
            String[] strings1 = string.split(":");
            if (!strings1[0].contains("value")){
                map.put(strings1[0], strings1[1]);
            }
        }
        return map;
    }
}

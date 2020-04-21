package com.dtc.java.analytic.V2.worker;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.*;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
@Slf4j
public class test {
    public static void main(String[] args) throws Exception {

        Map<String, String> map = new HashMap<>();
        map.put("10.3.7.231_1", "a");
        map.put("10.3.7.231_2", "b");
        map.put("10.3.7.231_3", "c");
        map.put("10.3.7.232_1", "a");
        map.put("10.3.7.232_2", "b");
        map.put("10.3.7.232_3", "c");
        String str = "123";
        double v = Double.parseDouble(str);
        System.out.println(v);

        String sjiachun = "12333";
        BigDecimal db = new BigDecimal(sjiachun);
        String ii = db.toPlainString();
        System.out.println(ii);
    }


}

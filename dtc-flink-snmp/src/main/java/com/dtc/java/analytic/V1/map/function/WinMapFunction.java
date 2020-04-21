package com.dtc.java.analytic.V1.map.function;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Created on 2020-02-04
 *
 * @author :hao.li
 */
public class WinMapFunction implements MapFunction<Tuple6<String, String, String, String, String, String>, Tuple7<String, String, String, String, String, String, String>> {

    @Override
    public Tuple7<String, String, String, String, String, String, String> map(Tuple6<String, String, String, String, String, String> event) throws Exception {

        if (event.f3.contains(".")) {
            String lastCode = event.f3.split("\\.", 2)[0];
            String nameCode = event.f3.split("\\.", 2)[1];
            String result = event.f2 + "_" + lastCode;
            return Tuple7.of(event.f0, event.f1, result, nameCode, event.f4, event.f5, "win_0");
        } else {
            return Tuple7.of(event.f0, event.f1, event.f2 + "_" + event.f3, event.f3, event.f4, event.f5, "win_1");
        }
    }
}

package com.dtc.java.analytic.V1.process.function;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2019-12-28
 *
 * @author :hao.li
 */
public class HWSwitchProcessMapFunction extends ProcessWindowFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple6<String, String, String, String, String, String>> iterable, Collector<Tuple6<String, String, String, String, String, String>> collector) throws Exception {

    }
}

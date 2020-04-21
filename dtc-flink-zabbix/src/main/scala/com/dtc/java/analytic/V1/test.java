package com.dtc.java.analytic.V1;

import com.dtc.java.analytic.V1.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V1.zabbix.PrometheusToFlink;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 2019-12-18
 *
 * @author :hao.li
 */

public class test {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusToFlink.class);

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');
        SingleOutputStreamOperator<Tuple2<String, String>> map = dataStreamSource.map(new testMapFunction());

        map.filter(e->e.f0.contains("h3c")).print();

        env.execute("Start zabbix data.");
    }
}

class testMapFunction implements MapFunction<String, Tuple2<String, String>> {

    @Override
    public Tuple2<String, String> map(String event) {
        Tuple2<String, String> message = null;
        String[] split = event.split(":");
        if(split[0].contains("a")) {
            message = Tuple2.of(split[0] + "h3c", split[1] + "d");
        }else {
            message=Tuple2.of(split[0] + "h4c", split[1] + "d");
        }
        return message;
    }
}

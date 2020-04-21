package com.dtc.java.analytic.V1.test1.broadcast;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
public class test {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<Map<String, String>> alarmDataStream = env.addSource(new GetAlarmNotifyData());//数据流定时从数据库中查出来数据

        //test for get data from MySQL
        alarmDataStream.print();


        env.execute("zhisheng broadcast demo");
    }
}

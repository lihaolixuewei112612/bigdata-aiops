package com.dtc.java.SC.DP;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.JSC.gldp.Lreand;
import com.dtc.java.SC.JSC.gldp.Lwrite;
import com.dtc.java.SC.WDZL.WdzlSink;
import com.dtc.java.SC.WDZL.WdzlSource;
import com.dtc.java.SC.common.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author : lihao
 * Created on : 2020-04-23
 * @Description : TODO描述类作用
 */
public class WDZComplete {
    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        int windowSizeMillis = Integer.parseInt(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.addSource(new Lreand()).addSink(new Lwrite());
//        //我的总览
        env.addSource(new WdzlSource()).addSink(new WdzlSink());

        env.execute("数仓-我的总览");
    }
}

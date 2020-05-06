package com.dtc.java.SC.ZHBB.common;


import com.dtc.java.SC.common.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
public class ExecutionEnvUtil_Batch {
    private static final String EXACTLY_ONCE_MODE = "exactly_once";
    private static final String EVENT_TIME = "EventTime";
    private static final String INGESTION_TIME = "IngestionTime";
    private static final String PROCESSING_TIME = "ProcessingTime";

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil_Batch.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil_Batch.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static ExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
         ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String eventType = parameterTool.get("dtc.eventType", "EventTime");
        String cpMode = parameterTool.get("dtc.checkpointMode", "exactly_once");
        //并行读为1
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 2));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        return env;
    }

    private static Map<String, String> getenv() {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, String> entry : System.getenv().entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }
        return map;
    }
}

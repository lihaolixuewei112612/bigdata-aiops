package com.dtc.java.analytic.V2.common.utils;


import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
/**
 * Created on 2020-02-15
 *
 * @author :hao.li
 */
public class ExecutionEnvUtil {
    private static final String EXACTLY_ONCE_MODE = "exactly_once";
    private static final String EVENT_TIME = "EventTime";
    private static final String INGESTION_TIME = "IngestionTime";
    private static final String PROCESSING_TIME = "ProcessingTime";

    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        return ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                .mergeWith(ParameterTool.fromArgs(args))
                .mergeWith(ParameterTool.fromSystemProperties())
                .mergeWith(ParameterTool.fromMap(getenv()));
    }

    public static final ParameterTool PARAMETER_TOOL = createParameterTool();

    private static ParameterTool createParameterTool() {
        try {
            return ParameterTool
                    .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
                    .mergeWith(ParameterTool.fromSystemProperties())
                    .mergeWith(ParameterTool.fromMap(getenv()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String eventType = parameterTool.get("dtc.eventType", "EventTime");
        String cpMode = parameterTool.get("dtc.checkpointMode", "exactly_once");
        //并行读为1
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 2));
        env.getConfig().disableSysoutLogging();
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        //每隔50s进行启动一个检查点
        if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
            env.enableCheckpointing(parameterTool.getInt(PropertiesConstants.STREAM_CHECKPOINT_INTERVAL, 10000)); // create a checkpoint every 5 seconds
        }
        env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface
        // 确保检查点之间有进行1s的进度
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        // 检查点必须在一分钟内完成，或者被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 同一时间只允许进行一个检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //开启checkpoints的外部持久化，但是在job失败的时候不会自动清理，需要自己手工清理state
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置模式为exactly-once （这是默认值）
        if (EXACTLY_ONCE_MODE.equals(cpMode)) {
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        }
//        env.setStateBackend(new
//                FsStateBackend("hdfs://hdfscluster:8020/DTC/flink/flink-checkpoints"));
        if (EVENT_TIME.equals(eventType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        } else if (PROCESSING_TIME.equals(eventType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        } else if (INGESTION_TIME.equals(eventType)) {
            env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        }
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

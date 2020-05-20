package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
import com.dtc.java.analytic.V2.map.function.WinMapFunction;
import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.WinProcessMapFunction;
import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
import com.dtc.java.analytic.V2.sink.opentsdb.PSinkToOpentsdb;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.dtc.java.analytic.V2.worker.StreamToFlinkV3.getAlarm;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
public class StreamToFlinkV3Test {
    private static final Logger logger = LoggerFactory.getLogger(StreamToFlinkV3.class);
    private static DataStream<Map<String, String>> alarmDataStream = null;
    static TimesConstats build = null;

    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                "alarm_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.10.58.16:4399");
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        int anInt_one = parameterTool.getInt("dtc.alarm.times.one", 1);
        int anInt1_one = parameterTool.getInt("dtc.alarm.time.long.one", 60000);
        int anInt_two = parameterTool.getInt("dtc.alarm.times.two", 1);
        int anInt1_two = parameterTool.getInt("dtc.alarm.time.long.two", 60000);
        build = TimesConstats.builder().one(anInt_one).two(anInt1_one).three(anInt_two).four(anInt1_two).build();
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        DataStreamSource<Tuple9<String, String, String, String, Double, String, String, String, String>> alarmMessageMysql = env.addSource(new TestSourceEvent()).setParallelism(1);
        DataStream<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> process = alarmMessageMysql.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new StreamToFlinkV3.MySqlProcessMapFunction());
        alarmDataStream = process.map(new StreamToFlinkV3.MySQLFunction());
        BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);

        DataStreamSource<SourceEvent> streamSource = env.addSource(new KafkaTestSourceEvent());

        /**
         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
         * */
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');

        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
////        SingleOutputStreamOperator<DataStruct> timeSingleOutputStream
////                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());
//
        SplitStream<DataStruct> splitStream
                = mapStream.split((OutputSelector<DataStruct>) event -> {
            List<String> output = new ArrayList<>();
            String type = event.getSystem_name();
            if ("101_100".equals(type) | "107_107".equals(type)) {
                output.add("Win");
            } else if ("101_101".equals(type)) {
                output.add("Linux");
            } else if ("102_101".equals(type)) {
                output.add("H3C_Switch");
            } else if ("102_102".equals(type)) {
                output.add("HW_Switch");
            } else if ("102_103".equals(type)) {
                output.add("ZX_Switch");
            } else if ("103_102".equals(type)) {
                output.add("DPI");
            }
            return output;
        });
        //windows指标数据处理
        SingleOutputStreamOperator<DataStruct> winProcess = splitStream
                .select("Win")
                .map(new WinMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new WinProcessMapFunction());
        //windows数据全量写opentsdb
        winProcess.addSink(new PSinkToOpentsdb(opentsdb_url));

        //windows数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmWindows = getAlarm(winProcess, broadcast, build);
        alarmWindows.forEach(e -> logger.info("windows告警：" + e));
        alarmWindows.forEach(e -> e.addSink(new MysqlSink()));

        //linux指标数据处理
        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
                .select("Linux")
                .map(new LinuxMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new LinuxProcessMapFunction());

        //Linux数据全量写opentsdb
        linuxProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
//
        //Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmLinux = getAlarm(linuxProcess, broadcast, build);
        alarmLinux.forEach(e -> e.print("linux打印告警:"));
        alarmLinux.forEach(e -> logger.info("linux日志告警：{}", e));

        alarmLinux.forEach(e -> e.addSink(new MysqlSink()));
        env.execute("Dtc-Alarm-Flink-Process");
    }
}




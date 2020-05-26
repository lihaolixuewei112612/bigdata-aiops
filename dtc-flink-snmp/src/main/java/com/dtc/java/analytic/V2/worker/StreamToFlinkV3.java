package com.dtc.java.analytic.V2.worker;

import com.dtc.java.analytic.V2.common.constant.HBaseConstant;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import com.dtc.java.analytic.V2.common.utils.ExecutionEnvUtil;
import com.dtc.java.analytic.V2.common.utils.KafkaConfigUtil;
import com.dtc.java.analytic.V2.map.function.DPIMapFunction;
import com.dtc.java.analytic.V2.map.function.H3cMapFunction;
import com.dtc.java.analytic.V2.map.function.LinuxMapFunction;
import com.dtc.java.analytic.V2.map.function.WinMapFunction;
import com.dtc.java.analytic.V2.process.function.DPISwitchProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.H3CSwitchProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.LinuxProcessMapFunction;
import com.dtc.java.analytic.V2.process.function.WinProcessMapFunction;
import com.dtc.java.analytic.V2.sink.mysql.MysqlSink;
import com.dtc.java.analytic.V2.sink.opentsdb.PSinkToOpentsdb;
import com.dtc.java.analytic.V2.source.mysql.ReadAlarmMessage;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.dtc.java.analytic.V2.alarm.AlarmUntils.getAlarm;


/**
 * Created on 2019-08-12
 *
 * @author :ren
 */
public class StreamToFlinkV3 {
    static BloomFilter<String> bf = BloomFilter.create(Funnels.stringFunnel(Charsets.UTF_8), 1000000, 0.001);
    private static final Logger logger = LoggerFactory.getLogger(StreamToFlinkV3.class);
    private static DataStream<Map<String, String>> alarmDataStream = null;
    static TimesConstats build = null;

    public static void main(String[] args) throws Exception {
        MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                "alarm_rules",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO);
        ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        String opentsdb_url = parameterTool.get("dtc.opentsdb.url", "http://10.10.58.16:4399");
        int windowSizeMillis = parameterTool.getInt("dtc.windowSizeMillis", 2000);
        getSize(parameterTool);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        env.getConfig().setGlobalJobParameters(parameterTool);
        //asset_id, ipv4, strategy_kind, triger_name, number, code, alarm_level, asset_code, name
        DataStreamSource<Tuple9<String, String, String, String, Double, String, String, String, String>> alarmMessageMysql = env.addSource(new ReadAlarmMessage()).setParallelism(1);
        DataStream<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> process = alarmMessageMysql.keyBy(0, 5).timeWindow(Time.milliseconds(windowSizeMillis)).process(new MySqlProcessMapFunction());
        alarmDataStream = process.map(new MySQLFunction());
        BroadcastStream<Map<String, String>> broadcast = alarmDataStream.broadcast(ALARM_RULES);

//        DataStreamSource<SourceEvent> streamSource = env.addSource(new TestSourceEvent());
        DataStreamSource<SourceEvent> streamSource = KafkaConfigUtil.buildSource(env);

        /**
         * {"time":"1581691002687","code":"101_101_107_105_105","host":"10.3.7.234","nameCN":"磁盘剩余大小","value":"217802544","nameEN":"disk_free"}
         * */
//        DataStreamSource<String> dataStreamSource = env.socketTextStream("172.20.10.2", 8080, '\n');

        SingleOutputStreamOperator<DataStruct> mapStream = streamSource.map(new MyMapFunctionV3());
//        SingleOutputStreamOperator<DataStruct> timeSingleOutputStream
//                = mapStream.assignTimestampsAndWatermarks(new DtcPeriodicAssigner());

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
        Win_Data_Process(opentsdb_url, windowSizeMillis, broadcast, splitStream);
        //linux指标数据处理
        Linux_Data_Process(opentsdb_url, windowSizeMillis, broadcast, splitStream);
        //h3c交换机处理
        H3c_Data_Process(opentsdb_url, windowSizeMillis, broadcast, splitStream, parameterTool);
        env.execute("Dtc-Alarm-Flink-Process");
    }

    private static void getSize(ParameterTool parameterTool) {
        int anInt_one = parameterTool.getInt("dtc.alarm.times.one", 1);
        int anInt1_one = parameterTool.getInt("dtc.alarm.time.long.one", 60000);
        int anInt_two = parameterTool.getInt("dtc.alarm.times.two", 1);
        int anInt1_two = parameterTool.getInt("dtc.alarm.time.long.two", 60000);
        build = TimesConstats.builder().one(anInt_one).two(anInt1_one).three(anInt_two).four(anInt1_two).build();
    }

    private static void Win_Data_Process(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream) {
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
        alarmWindows.forEach(e -> e.addSink(new MysqlSink()));
    }

    private static void Linux_Data_Process(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream) {
        SingleOutputStreamOperator<DataStruct> linuxProcess = splitStream
                .select("Linux")
                .map(new LinuxMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new LinuxProcessMapFunction());

        //Linux数据全量写opentsdb
        linuxProcess.addSink(new PSinkToOpentsdb(opentsdb_url));
        //Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> alarmLinux = getAlarm(linuxProcess, broadcast, build);
        alarmLinux.forEach(e -> e.addSink(new MysqlSink()));
    }

    private static void H3c_Data_Process(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool) {
        //交换机指标数据处理
        SingleOutputStreamOperator<DataStruct> H3C_Switch = splitStream
                .select("H3C_Switch")
                .map(new H3cMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new H3CSwitchProcessMapFunction());
        H3C_Switch.map(new MapFunction<DataStruct, Object>() {
            @Override
            public Object map(DataStruct string) throws Exception {
                String demo = string.getHost() + "_" + string.getZbFourName() + "_" + string.getZbLastCode();
                if (!bf.mightContain(demo)) {
                    if ("102_101_101_101_101".equals(string.getZbFourName())) {
                        bf.put(demo);
                        writeEventToHbase(string, parameterTool, "1");
                    }
                    if ("102_101_103_107_108".equals(string.getZbFourName())) {
                        bf.put(demo);
                        writeEventToHbase(string, parameterTool, "2");
                    }
                }
                return string;
            }
        });
        //Linux数据全量写opentsdb
        H3C_Switch.addSink(new PSinkToOpentsdb(opentsdb_url));
        //Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> H3C_Switch_1 = getAlarm(H3C_Switch, broadcast, build);
        H3C_Switch_1.forEach(e -> e.addSink(new MysqlSink()));
    }

    private static void DPI_Data_Process(String opentsdb_url, int windowSizeMillis, BroadcastStream<Map<String, String>> broadcast, SplitStream<DataStruct> splitStream, ParameterTool parameterTool) {
        //交换机指标数据处理
        SingleOutputStreamOperator<DataStruct> H3C_Switch = splitStream
                .select("DPI")
                .map(new DPIMapFunction())
                .keyBy("Host")
                .timeWindow(Time.of(windowSizeMillis, TimeUnit.MILLISECONDS))
                .process(new DPISwitchProcessMapFunction());
        H3C_Switch.map(new MapFunction<DataStruct, Object>() {
            @Override
            public Object map(DataStruct string) throws Exception {
                String demo = string.getHost() + "_" + string.getZbFourName() + "_" + string.getZbLastCode();
                if (!bf.mightContain(demo)) {
                    if ("103_102_101_101_101".equals(string.getZbFourName())) {
                        bf.put(demo);
                        writeEventToHbase(string, parameterTool, "1");
                    }
                    if ("103_102_103_107_107_1".equals(string.getZbFourName())) {
                        bf.put(demo);
                        writeEventToHbase(string, parameterTool, "2");
                    }
                }
                return string;
            }
        });
        //Linux数据全量写opentsdb
        H3C_Switch.addSink(new PSinkToOpentsdb(opentsdb_url));
        //Linux数据进行告警规则判断并将告警数据写入mysql
        List<DataStream<AlterStruct>> H3C_Switch_1 = getAlarm(H3C_Switch, broadcast, build);
        H3C_Switch_1.forEach(e -> e.addSink(new MysqlSink()));
    }


    private static void writeEventToHbase(DataStruct string, ParameterTool parameterTool, String str) throws IOException {
        TableName HBASE_TABLE_NAME = null;
        String INFO_STREAM = null;
        String BAR_STREAM = null;
        if ("1" == str) {
            HBASE_TABLE_NAME = TableName.valueOf("switch_1");
            //列族
            INFO_STREAM = "banka";
            //列名
            BAR_STREAM = "bk";
        }
        if ("2" == str) {
            HBASE_TABLE_NAME = TableName.valueOf("switch_2");
            //列族
            INFO_STREAM = "jiekou";
            //列名
            BAR_STREAM = "jk";
        }
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_QUORUM, "10.3.7.232,10.3.7.233,10.3.6.20");
        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, "2181");
        configuration.set(HBaseConstant.HBASE_RPC_TIMEOUT, "20000");
        Connection connect = ConnectionFactory.createConnection(configuration);
//        Admin admin = connect.getAdmin();
//        if (!admin.tableExists(HBASE_TABLE_NAME)) { //检查是否有该表，如果没有，创建
//            admin.createTable(new HTableDescriptor(HBASE_TABLE_NAME).addFamily(new HColumnDescriptor(INFO_STREAM)));
//        }
        Table table = connect.getTable(HBASE_TABLE_NAME);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        String host = string.getHost();
        String code = string.getZbFourName();
        String zbLastCode = string.getZbLastCode();
        Put put = new Put(Bytes.toBytes(host+"_"+code+"_"+zbLastCode));
        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes(BAR_STREAM), Bytes.toBytes(zbLastCode));
        table.put(put);
        table.close();
        connect.close();
    }


    static class MySQLFunction implements MapFunction<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Map<String, String>> {
        //(445,10.3.1.6,101_101_106_103,50.0,null,null,null)

        @Override
        public Map<String, String> map(Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> event) throws Exception {
            Map<String, String> map = new HashMap<>();
            for (Map.Entry<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> entries : event.entrySet()) {
                Tuple9<String, String, String, Double, Double, Double, Double, String, String> value = entries.getValue();
                String key = entries.getKey();
                String asset_id = value.f0;
                String ip = value.f1;
                String code = value.f2;
                Double level_1 = value.f3;
                Double level_2 = value.f4;
                Double level_3 = value.f5;
                Double level_4 = value.f6;
                String asset_code = value.f7;
                String asset_name = value.f8;
                String str = asset_id + ":" + code + ":" + asset_code + ":" + asset_name + ":" + level_1 + "|" + level_2 + "|" + level_3 + "|" + level_4;
                map.put(key, str);
            }
            return map;
        }
    }

    @Slf4j
    static class MySqlProcessMapFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Double, String, String, String, String>, Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Tuple, TimeWindow> {
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple9<String, String, String, String, Double, String, String, String, String>> iterable, Collector<Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>> collector) throws Exception {
            //asset_id, ipv4, strategy_kind, triger_name, number, code, alarm_level, asset_code, name
            Tuple9<String, String, String, Double, Double, Double, Double, String, String> tuple9 = new Tuple9<>();
            Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>> map = new HashMap<>();
            for (Tuple9<String, String, String, String, Double, String, String, String, String> sourceEvent : iterable) {
                String asset_id = sourceEvent.f0;
                String ip = sourceEvent.f1;
                Double num = sourceEvent.f4;
                String code = sourceEvent.f5;
                String level = sourceEvent.f6;
                tuple9.f0 = asset_id;
                tuple9.f1 = ip;
                tuple9.f2 = code;
                String key = ip + "." + code.replace("_", ".");
                if ("1".equals(level)) {
                    tuple9.f3 = num;
                } else if ("2".equals(level)) {
                    tuple9.f4 = num;
                } else if ("3".equals(level)) {
                    tuple9.f5 = num;
                } else if ("4".equals(level)) {
                    tuple9.f6 = num;
                }
                tuple9.f7 = sourceEvent.f7;
                tuple9.f8 = sourceEvent.f8;
                map.put(key, tuple9);
            }
            collector.collect(map);
        }

    }
}


@Slf4j
class MyMapFunctionV3 implements MapFunction<SourceEvent, DataStruct> {
    @Override
    public DataStruct map(SourceEvent sourceEvent) {
        String[] codes = sourceEvent.getCode().split("_");
        String systemName = codes[0].trim() + "_" + codes[1].trim();
        String zbFourCode = systemName + "_" + codes[2].trim() + "_" + codes[3].trim();
        String zbLastCode = codes[4].trim();
        String nameCN = sourceEvent.getNameCN();
        String nameEN = sourceEvent.getNameEN();
        String time = sourceEvent.getTime();
        String value = sourceEvent.getValue();
        String host = sourceEvent.getHost();
        return new DataStruct(systemName, host, zbFourCode, zbLastCode, nameCN, nameEN, time, value);
    }
}


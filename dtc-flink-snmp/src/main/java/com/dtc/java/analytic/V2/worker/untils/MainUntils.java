package com.dtc.java.analytic.V2.worker.untils;

import com.dtc.java.analytic.V2.common.constant.HBaseConstant;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.SourceEvent;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
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

import java.io.IOException;
import java.util.*;

/**
 * @Author : lihao
 * Created on : 2020-05-26
 * @Description : TODO描述类作用
 */
public class MainUntils {

    public static SplitStream<DataStruct> getSplit(SingleOutputStreamOperator<DataStruct> mapStream) {
        return mapStream.split((OutputSelector<DataStruct>) event -> {
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
    }

    public static TimesConstats getSize(ParameterTool parameterTool) {
        int anInt_one = parameterTool.getInt("dtc.alarm.times.one", 1);
        int anInt1_one = parameterTool.getInt("dtc.alarm.time.long.one", 60000);
        int anInt_two = parameterTool.getInt("dtc.alarm.times.two", 1);
        int anInt1_two = parameterTool.getInt("dtc.alarm.time.long.two", 60000);
        TimesConstats build = TimesConstats.builder().one(anInt_one).two(anInt1_one).three(anInt_two).four(anInt1_two).build();
        return build;
    }

    public static void writeEventToHbase(DataStruct string, ParameterTool parameterTool, String str) throws IOException {
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
        Put put = new Put(Bytes.toBytes(host + "_" + code + "_" + zbLastCode));
        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes(BAR_STREAM), Bytes.toBytes(zbLastCode));
        table.put(put);
        table.close();
        connect.close();
    }

    @Slf4j
    public static class MySqlProcessMapFunction extends ProcessWindowFunction<Tuple9<String, String, String, String, Double, String, String, String, String>, Map<String, Tuple9<String, String, String, Double, Double, Double, Double, String, String>>, Tuple, TimeWindow> {
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

    @Slf4j
    public static class MyMapFunctionV3 implements MapFunction<SourceEvent, DataStruct> {
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

}

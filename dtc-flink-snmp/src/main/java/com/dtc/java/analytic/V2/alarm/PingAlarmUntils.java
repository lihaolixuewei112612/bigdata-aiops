package com.dtc.java.analytic.V2.alarm;

/**
 * @Author : lihao
 * Created on : 2020-05-26
 * @Description : TODO描述类作用
 */

import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import com.dtc.java.analytic.V2.common.model.TimesConstats;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @Author : lihao
 * Created on : 2020-05-22
 * @Description : TODO描述类作用
 */
public class PingAlarmUntils {
    public static DataStream<AlterStruct> getAlarmPing(SingleOutputStreamOperator<DataStruct> event, BroadcastStream<Map<String, String>> broadcast, TimesConstats test) {

        SingleOutputStreamOperator<AlterStruct> alert_rule = event.filter(e->"107_107_101_101_101".equals(e.getZbFourName())).connect(broadcast)
                .process(getAlarmFunction1());
        return alert_rule;
    }
    private static BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct> getAlarmFunction1() {
        return new BroadcastProcessFunction<DataStruct, Map<String, String>, AlterStruct>() {
            MapStateDescriptor<String, String> ALARM_RULES = new MapStateDescriptor<>(
                    "alarm_rules",
                    BasicTypeInfo.STRING_TYPE_INFO,
                    BasicTypeInfo.STRING_TYPE_INFO);

            @Override
            public void processElement(DataStruct value, ReadOnlyContext ctx, Collector<AlterStruct> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                String host_ip = value.getHost();
                String code = value.getZbFourName().replace("_", ".");
                String weiyi = host_ip + "." + code;
                if (!broadcastState.contains(weiyi)) {
                    return;
                }
                //asset_id + ":" + code + ":"+ asset_code + ":" +asset_name+":"+ alarm
                String targetId = broadcastState.get(weiyi).trim();
                String[] split = targetId.split(":");
                if (split.length != 5) {
                    return;
                }
                String unique_id = split[0].trim();
                String r_code = split[1].trim();
                if (!r_code.equals(value.getZbFourName())) {
                    return;
                }
                String asset_code = split[2].trim();
                String asset_name = split[3].trim();
                String result = asset_code + "(" + asset_name + ")";
                String r_value = split[4].trim();
                if (unique_id.isEmpty() || code.isEmpty() || r_value.isEmpty()) {
                    return;
                }
                String[] split1 = r_value.split("\\|");
                if (split1.length != 4) {
                    return;
                }
                broadcastState.clear();
                AlarmRule1(value, out, unique_id, split1, result);
            }

            @Override
            public void processBroadcastElement(Map<String, String> value, Context ctx, Collector<AlterStruct> out) throws Exception {
                if (value == null || value.size() == 0) {
                    return;
                }
                if (value != null) {
                    BroadcastState<String, String> broadcastState = ctx.getBroadcastState(ALARM_RULES);
                    for (Map.Entry<String, String> entry : value.entrySet()) {
                        broadcastState.put(entry.getKey(), entry.getValue());
                    }
                }
            }
        };
    }
    /**
     * 告警规则
     */
    public static void AlarmRule1(DataStruct value, Collector<AlterStruct> out, String unique_id, String[] split1, String str1) {
        double data_value = Double.parseDouble(value.getValue());
        String code_name = str1;
        String level_1 = split1[0];
        String level_2 = split1[1];
        String level_3 = split1[2];
        String level_4 = split1[3];
        //一个不为空，其他的都为空
        if (!("null".equals(level_1)) && "null".equals(level_2) && "null".equals(level_3) && "null".equals(level_4)) {
            Double num_1 = Double.parseDouble(split1[0]);
            if ((data_value < num_1 )) {
                String system_time = String.valueOf(System.currentTimeMillis());
                AlterStruct alter_message = new AlterStruct(code_name, value.getHost(), value.getZbFourName(), value.getZbLastCode(), value.getNameCN(), value.getNameEN(), value.getTime(), system_time, value.getValue(), "1", unique_id, String.valueOf(num_1), value.getHost() + "-" + value.getZbFourName());
                out.collect(alter_message);
            }
        }
    }
}


package com.dtc.java.analytic.V2.alarm;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2019-12-26
 * 告警收敛规则,测试发现有bug，需要进一步完善
 *
 * @author :hao.li
 */
@Slf4j
public class timeAlarmConvergence extends ProcessWindowFunction<AlterStruct, AlterStruct, Tuple, TimeWindow> {
    /**
     * 此处的map<code(in.f2.in.f3),value_time>
     */
    Map<Long, Integer> mapSwitch = new HashMap<>();


    //    Map<String, Integer> mapSwitch_bak = new HashMap<>();
    @Override
    public void process(Tuple tuple, Context context, Iterable<AlterStruct> iterable, Collector<AlterStruct> collector) throws Exception {
        Map<String, Integer> mapSwitch_bak = new HashMap<>();
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int anInt = parameters.getInt(PropertiesConstants.TIME_ALARM_CONVERGENCE) * 60 * 1000;
        for (AlterStruct in : iterable) {
            String code = in.getZbFourName();
            //判断是否是数据
            boolean strResult = in.getValue().matches("-?[0-9]+.*[0-9]*");
            if (!strResult) {
                log.info("Value is not number of string!");
            } else {
                if (mapSwitch.size() == 0) {
                    AlterStruct alter_message1 = new AlterStruct("test-1", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message1);
                } else {
                    long l = Long.parseLong(in.getEvent_time());
                    if (l - Long.parseLong(getMinKey(mapSwitch).toString()) > anInt && mapSwitch.size() > 1) {
                        AlterStruct alter_message1 = new AlterStruct("test-1", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                        collector.collect(alter_message1);
                        mapSwitch.clear();
                    }
                }
                mapSwitch.put(Long.parseLong(in.getEvent_time()), 1);
            }

        }
        //中间隔断，上周期的数据需要清空
//        Iterator<Map.Entry<String, Integer>> iterator = mapSwitch.entrySet().iterator();
//        while(iterator.hasNext()){
//            Map.Entry<String, Integer> next = iterator.next();
//            String key = next.getKey();
//            if (!mapSwitch_bak.containsKey(key)) {
//                iterator.remove();
//            }
//        }
//        mapSwitch_bak.clear();
    }

    public static Object getMinKey(Map<Long, Integer> map) {
        if (map == null) return null;
        Set<Long> set = map.keySet();
        Object[] obj = set.toArray();
        Arrays.sort(obj);
        return obj[0];
    }
}



package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2019-12-26
 * 告警收敛规则
 *
 * @author :hao.li
 */
@Slf4j
public class alarmConvergence extends ProcessWindowFunction<AlterStruct, AlterStruct, Tuple, TimeWindow> {
    /**
     * 此处的map<code(in.f2.in.f3),value_time>
     *
     * */
    Map<String, Integer> mapSwitch = new HashMap<>();
    @Override
    public void process(Tuple tuple, Context context, Iterable<AlterStruct> iterable, Collector<AlterStruct> collector) throws Exception {
        Map<String, Integer> mapSwitch_bak = new HashMap<>();
        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        int anInt = parameters.getInt(PropertiesConstants.ALARM_CONVERGENCE);
        for (AlterStruct in : iterable) {
            String code = in.getZbFourName();
            //判断是否是数据
            boolean strResult = in.getValue().matches("-?[0-9]+.*[0-9]*");
            if (!strResult) {
                log.info("Value is not number of string!");
            } else {
                mapSwitch_bak.put(in.getGaojing(),0);
                mapSwitch.put(in.getGaojing(),mapSwitch.getOrDefault(in.getGaojing(),0)+1);
                if(mapSwitch.get(in.getGaojing())==(anInt*0)+1){
                    AlterStruct alter_message = new AlterStruct("test-1", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*1)+1){
                    AlterStruct alter_message = new AlterStruct("test-2", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                } else if(mapSwitch.get(in.getGaojing())==(anInt*2)+1){
                    AlterStruct alter_message = new AlterStruct("test-3", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*4)+1){
                    AlterStruct alter_message = new AlterStruct("test-4", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*8)+1){
                    AlterStruct alter_message = new AlterStruct("test-5", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*16)+1){
                    AlterStruct alter_message = new AlterStruct("test-6", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*32)+1){
                    AlterStruct alter_message = new AlterStruct("test-7", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*64)+1){
                    AlterStruct alter_message = new AlterStruct("test-8", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*128)+11){
                    AlterStruct alter_message = new AlterStruct("test-9", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*256)+1){
                    AlterStruct alter_message = new AlterStruct("test-10", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
                else if(mapSwitch.get(in.getGaojing())==(anInt*512)+1){
                    AlterStruct alter_message = new AlterStruct("test-11", in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getEvent_time(), in.getSystem_time(), in.getValue(), "1", in.getUnique_id(), in.getYuzhi(), in.getHost() + "-" + in.getZbFourName());
                    collector.collect(alter_message);
                }
            }

        }
        //中间隔断，上周期的数据需要清空
        Set<Map.Entry<String, Integer>> entries = mapSwitch.entrySet();
        for (Map.Entry<String, Integer> next : entries) {
            String key = next.getKey();
            if (!mapSwitch_bak.containsKey(key)) {
                mapSwitch.remove(key);
            }
        }
    }
}



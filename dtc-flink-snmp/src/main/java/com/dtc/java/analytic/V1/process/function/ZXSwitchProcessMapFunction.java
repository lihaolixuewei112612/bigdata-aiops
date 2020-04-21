package com.dtc.java.analytic.V1.process.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Created on 2019-12-28
 *
 * @author :hao.li
 */
@Slf4j
public class ZXSwitchProcessMapFunction extends ProcessWindowFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple, TimeWindow> {

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple6<String, String, String, String, String, String>> iterable, Collector<Tuple6<String, String, String, String, String, String>> collector) throws Exception {
        for (Tuple6<String, String, String, String, String, String> in : iterable) {
            String code = in.f2;
            //判断是否是数据
            boolean strResult = in.f5.matches("-?[0-9]+.*[0-9]*");
            if (!strResult) {
                log.info("value is not number of string!");
            } else if (code.equals("102_103_101_101_101") || code.equals("102_103_101_102_102")
                    || code.equals("102_103_102_103_103") || code.equals("102_103_103_107_107")
                    || code.equals("102_103_103_108_108") || code.equals("102_103_103_109_109")
                    || code.equals("102_103_103_110_110") || code.equals("102_103_103_111_111")) {
                /**
                 * 交换机cpu使用率/内存总量
                 * /内存使用率/端口入方向字节数
                 * /端口入方向错误报文数/端口出方向字节数
                 * /端口出方向错误报文数/端口的物理状态
                 * 以交换机cpu使用率数据格式为例说明如下：
                 * (system_name,ip,ZN_Name,ZB_code,time,value)
                 * (102_101,ip,102_101_101_101_101,101.1.0,time,value)
                 *其中ZB_code为例：机器ip,1.0表示机框1，0板卡，表示机框1，板卡0的cpu使用率
                 */
                collector.collect(Tuple6.of(in.f0 + "zx", in.f1, in.f2, in.f3, in.f4, in.f5));
                continue;
            }
            //端口出入速率指标
            if (code.equals("102_103_103_105_105") || code.equals("102_103_103_106_106")) {
                String f3 = asciiToString(in.f3);
                collector.collect(Tuple6.of(in.f0 + "zx", in.f1, in.f2, f3, in.f4, in.f5));
                continue;


            }
        }
    }

    /**
     * ascii码转化string
     */
    public static String asciiToString(String value) {
        StringBuffer sbu = new StringBuffer();
        String[] chars = value.split("\\.");
        for (int i = 0; i < chars.length; i++) {
            sbu.append((char) Integer.parseInt(chars[i]));
        }
        return sbu.toString().replace("/", ".");
    }
}

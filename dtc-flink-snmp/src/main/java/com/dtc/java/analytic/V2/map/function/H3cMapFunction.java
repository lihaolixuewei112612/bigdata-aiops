package com.dtc.java.analytic.V2.map.function;


import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created on 2020-02-021
 *
 * @author :hao.li
 */
public class H3cMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String fourCode = event.getZbFourName();
        String LastCode = event.getZbLastCode();
        if (LastCode.contains(".")) {
            String lastCode = fourCode+"_"+LastCode.split("\\.", 2)[0];
            if ("102_101_101_101_101".equals(lastCode) || "102_101_102_102_102".equals(lastCode) || "201_101_102_103_103".equals(lastCode)) {
                String nameCode = LastCode.split("\\.", 2)[1];
                //板卡
                return new DataStruct(event.getSystem_name() + "|h3c_0", event.getHost(), lastCode, nameCode, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());

            }
            if ("102_101_103_106_107".equals(lastCode) || "102_101_103_107_108".equals(lastCode) ||
                    "102_101_103_108_109".equals(lastCode) || "102_101_103_109_110".equals(lastCode) || "102_101_103_110_111".equals(lastCode)) {
                String nameCode = LastCode.split("\\.", 2)[1];
                //端口
                return new DataStruct(event.getSystem_name() + "|h3c_1", event.getHost(), lastCode, nameCode, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
            }
        }
        return new DataStruct(event.getSystem_name() + "|h3c_2", event.getHost(), event.getZbFourName() + "_" + event.getZbLastCode(), "", event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
    }
}

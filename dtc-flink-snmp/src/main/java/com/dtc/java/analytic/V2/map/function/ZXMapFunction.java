package com.dtc.java.analytic.V2.map.function;


import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created on 2020-02-021
 *
 * @author :hao.li
 */
public class ZXMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String fourCode = event.getZbFourName();
        String LastCode = event.getZbLastCode();
        if (LastCode.contains(".")) {
            String lastCode = fourCode+"_"+LastCode.split("\\.", 2)[0];
            if ("102_103_101_101_101".equals(lastCode) || "102_103_101_102_102".equals(lastCode) || "102_103_102_103_103".equals(lastCode)) {
                String nameCode = LastCode.split("\\.", 2)[1];
                //板卡
                return new DataStruct(event.getSystem_name() + "|zx_0", event.getHost(), lastCode, nameCode, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());

            }
            if ("102_103_103_105_105".equals(lastCode) || "102_103_103_106_106".equals(lastCode) ||
                    "102_103_103_107_107".equals(lastCode) || "102_103_103_108_108".equals(lastCode) || "102_103_103_109_109".equals(lastCode)
                    || "102_103_103_110_110".equals(lastCode)) {
                String nameCode = LastCode.split("\\.", 2)[1];
                //端口
                return new DataStruct(event.getSystem_name() + "|zx_1", event.getHost(), lastCode, nameCode, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
            }
        }
        return new DataStruct(event.getSystem_name() + "|zx_2", event.getHost(), event.getZbFourName() + "_" + event.getZbLastCode(), "", event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
    }
}

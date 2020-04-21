package com.dtc.java.analytic.V2.map.function;


import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created on 2020-02-021
 *
 * @author :hao.li
 */
public class LinuxMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();
        if (zbLastCode.contains(".")) {
            String lastCode = zbLastCode.split("\\.", 2)[0];
            String nameCode = zbLastCode.split("\\.", 2)[1];
            String result = event.getZbFourName() + "_" + lastCode;
            return new DataStruct(event.getSystem_name() + "|linux_0", event.getHost(), result, nameCode, event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        } else {
            return new DataStruct(event.getSystem_name() + "|linux_1", event.getHost(), event.getZbFourName() + "_" + event.getZbLastCode(), "", event.getNameCN(), event.getNameEN(), event.getTime(), event.getValue());
        }
    }
}

package com.dtc.java.analytic.V2.map.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

/**
 * Created on 2020-02-21
 *
 * @author :hao.li
 */
public class WinMapFunction implements MapFunction<DataStruct, DataStruct> {

    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();

        if (zbLastCode.contains(".")) {
            String lastCode = zbLastCode.split("\\.", 2)[0];
            String nameCode = zbLastCode.split("\\.", 2)[1];
            String result = event.getZbFourName() + "_" + lastCode;
            return new DataStruct(event.getSystem_name() + "|win_0",event.getHost(),result,nameCode,event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());

        } else {
            String result = event.getZbFourName() + "_" + zbLastCode;
            return new DataStruct(event.getSystem_name() + "|win_1",event.getHost(),result,"",event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());

        }
    }
}

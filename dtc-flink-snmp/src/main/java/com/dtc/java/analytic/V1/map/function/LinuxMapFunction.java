package com.dtc.java.analytic.V1.map.function;

import com.dtc.java.analytic.V1.snmp.DataStruct;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created on 2020-02-04
 *
 * @author :hao.li
 */
public class LinuxMapFunction implements MapFunction<DataStruct, DataStruct> {
/*
* private String System_name;
    private String Host;
    private String zbFourName;
    private String zbLastCode;
    private String nameCN;
    private String nameEN;
    private String time;
    private String value;
* */    @Override
    public DataStruct map(DataStruct event) throws Exception {
        String zbLastCode = event.getZbLastCode();
        if (zbLastCode.contains(".")) {
            String lastCode =zbLastCode.split("\\.", 2)[0];
            String nameCode = zbLastCode.split("\\.", 2)[1];
            String result = event.getZbFourName() + "_" + lastCode;
            return new DataStruct(event.getSystem_name() + "|linux_0",event.getHost(),result,nameCode,event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());
        } else {
            return new DataStruct(event.getSystem_name() + "|linux_1",event.getHost(),event.getZbFourName()+"_"+event.getZbLastCode(),event.getZbLastCode(),event.getNameCN(),event.getNameEN(),event.getTime(),event.getValue());
        }
    }
}

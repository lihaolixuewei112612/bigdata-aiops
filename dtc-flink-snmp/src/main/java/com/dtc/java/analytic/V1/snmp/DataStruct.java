package com.dtc.java.analytic.V1.snmp;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2019-08-12
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataStruct {
    private String System_name;
    private String Host;
    private String zbFourName;
    private String zbLastCode;
    private String nameCN;
    private String nameEN;
    private String time;
    private String value;
}

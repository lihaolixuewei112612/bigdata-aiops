package com.dtc.java.analytic.V2.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2020-02-15
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

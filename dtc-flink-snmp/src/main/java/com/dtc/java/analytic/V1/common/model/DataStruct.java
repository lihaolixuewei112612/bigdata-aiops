package com.dtc.java.analytic.V1.common.model;

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

    public String System_name;
    public String Host;
    public String ZB_Name;
    public String ZB_Code;
    public String time;
    public Double value;
}

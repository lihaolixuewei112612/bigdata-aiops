package com.dtc.java.analytic.V1.common.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created on 2019-12-20
 *
 * @author :hao.li
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class MetricEvent {
    private String time;
    private String code;
    private String host;
    private String nameCN;
    private String value;
    private String nameEN;
}

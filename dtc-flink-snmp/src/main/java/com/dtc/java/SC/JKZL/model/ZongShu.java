package com.dtc.java.SC.JKZL.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : TODO描述类作用
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class ZongShu {
    private String room;
    private String partitions;
    private String box;
    private double num;
    private int flag;
}

package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * @author ChenTong
 */
@Description(
        name = "GetZsnlpf",		   // 择时能力评分
        value = "_FUNC_(DoubleWritable, DoubleWritable, IntWritable, IntWritable) - 根据类型返回 DoubleWritable"
)

public class GetZsnlpf extends UDF{
    public Double evaluate(Double zscgl, Double ztcgl, IntWritable czcs, IntWritable ztcs) {
        return LabelClassfier.getZsnlpf(zscgl,ztcgl,Integer.parseInt(czcs.toString()),Integer.parseInt(ztcs.toString()));
    }
}



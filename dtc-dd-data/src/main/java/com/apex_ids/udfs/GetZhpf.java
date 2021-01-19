package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;

/**
 * @author ChenTong
 */
@Description(
        name = "GetZhpf",		   // 综合评分
        value = "_FUNC_(DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable,DoubleWritable) - 根据类型返回 DoubleWritable"
)

public class GetZhpf extends UDF{
    public Double evaluate(Double yl, Double fk, Double pm, Double zs, Double xg){
        return LabelClassfier.getZhpf(yl,fk,pm,zs,xg);
    }
}

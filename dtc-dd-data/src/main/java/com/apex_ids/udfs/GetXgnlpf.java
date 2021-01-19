package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;


/**
 * @author ChenTong
 */
@Description(
        name = "GetXgnlpf",		   // 选股能力评分
        value = "_FUNC_(DoubleWritable) - 根据类型返回DoubleWritable"
)

public class GetXgnlpf extends UDF{
    public Double evaluate(Double xgcgl) {
        return LabelClassfier.getXgnlpf(xgcgl);
    }
}


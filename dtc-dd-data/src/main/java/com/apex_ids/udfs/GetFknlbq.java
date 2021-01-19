package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetFknlbq",		   // 风控能力标签
        value = "_FUNC_(Text, DoubleWritable) - 根据类型返回 Text"
)

public class GetFknlbq extends UDF{
    public Text evaluate(Text bq, Double sz){
        return new Text(LabelClassfier.getFkbq(bq.toString(), sz));
    }
}

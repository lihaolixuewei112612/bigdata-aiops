package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetFkbq",		   // 风控标签
        value = "_FUNC_(Text, Text) - 根据类型返回 Text"
)

public class GetFkbq extends UDF{
    public Text evaluate(Text bq, DoubleWritable sz){
        return new Text(LabelClassfier.getFkbq(bq.toString(),
                                               Double.parseDouble(sz.toString())));
    }
}

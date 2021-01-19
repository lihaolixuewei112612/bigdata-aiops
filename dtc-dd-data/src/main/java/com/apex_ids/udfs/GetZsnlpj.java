package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColEqualLongColumn;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetZsnlpj",		   // 择时能力评价
        value = "_FUNC_(DoubleWritable, DoubleWritable, IntWritable, IntWritable) - 根据类型返回 Text"
)

public class GetZsnlpj extends UDF {
    public Text evaluate(Double zscgl, Double ztcgl, IntWritable czcs, IntWritable ztcs) {
        return new Text(LabelClassfier.getZsnlpj(zscgl,ztcgl,Integer.parseInt(czcs.toString()),Integer.parseInt(ztcs.toString())));
    }
}

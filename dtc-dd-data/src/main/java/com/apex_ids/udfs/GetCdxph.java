package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "getCdxph",		   // 长短线偏好
        value = "_FUNC_(IntWritable, IntWritable, IntWritable) - 根据类型返回 Text"
)

public class GetCdxph extends UDF{
    public Text evaluate(IntWritable dxcg, IntWritable zxcg, IntWritable cxcg){
        return new Text(LabelClassfier.getCdxcc(Integer.parseInt(dxcg.toString()),
                                                Integer.parseInt(zxcg.toString()),
                                                Integer.parseInt(cxcg.toString())));
    }
}

package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetZycph",		   // 左右侧偏好
        value = "_FUNC_(IntWritable, IntWritable) - 根据类型返回 Text"
)

public class GetZycph extends UDF{
    public Text evaluate(IntWritable zc, IntWritable yc){
        return new Text(LabelClassfier.getZycjy(Integer.parseInt(zc.toString()),
                                                Integer.parseInt(yc.toString())));
    }
}


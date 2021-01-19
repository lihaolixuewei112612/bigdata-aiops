package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetFxbx",		   // 风险表现标签
        value = "_FUNC_(Text, Text) - 根据类型返回 Text"
)

public class GetFxbx extends UDF{
    public Text evaluate(Text bdbq, Text hcbq){
        return new Text(LabelClassfier.getFxbx(bdbq.toString(),
                                               hcbq.toString()));
    }
}

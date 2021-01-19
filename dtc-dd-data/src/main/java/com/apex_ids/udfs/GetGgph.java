package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetGgph",		   // 个股偏好
        value = "_FUNC_(Text) - 根据类型返回 Text"
)
public class GetGgph extends UDF{
    public Text evaluate(Text ggph) {
        return new Text(LabelClassfier.getGgph(ggph.toString()));
    }
}

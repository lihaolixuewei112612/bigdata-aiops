package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetHyph",		   // 行业偏好
        value = "_FUNC_(Text) - 根据类型返回 Text"
)

public class GetHyph extends UDF{
    public Text evaluate(Text hyph) {
        return new Text(LabelClassfier.getHyph(hyph.toString()));
    }
}

package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetDxpph",		   // 大小盘偏好
        value = "_FUNC_(Text) - 根据类型返回 Text"
)

public class GetDxpph extends UDF{
    public Text evaluate(Text tzfg){
        return new Text(LabelClassfier.getDxpph(tzfg.toString()));
    }
}

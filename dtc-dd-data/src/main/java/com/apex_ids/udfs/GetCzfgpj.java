package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "GetCzfgpj",		   // 操作风格评价
        value = "_FUNC_(Text, Text, Text, Text) - 根据类型返回 Text"
)

public class GetCzfgpj extends UDF{
    public Text evaluate(Text cdx, Text zyc, Text fxbx, Text dxpph) {
        return new Text(LabelClassfier.getCzfgZhpj(cdx.toString(),
                                                   zyc.toString(),
                                                   fxbx.toString(),
                                                   dxpph.toString()));
    }
}

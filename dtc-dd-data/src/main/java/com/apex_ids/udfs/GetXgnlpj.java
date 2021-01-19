package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

/**
 * @author ChenTong
 */
@Description(
        name = "getXgnlpj",		   // 选股能力评价
        value = "_FUNC_(Double) - 根据类型返回Text"
)

public class GetXgnlpj extends UDF {
    public Text evaluate(Double xgcgl) {
        return new Text(LabelClassfier.getXgnlpj(xgcgl));
    }
}

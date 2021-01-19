package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.lang.String;

/**
 * @author ChenTong
 */
@Description(
        name = "GetCountSet",
        value = "_FUNC_(String, String) - 根据类型返回Int"
)

public class GetCountSet extends UDF{
    public IntWritable evaluate(Text strings, Text splitChar) throws Exception{
        String[] datas = strings.toString().split(splitChar.toString());
        return new IntWritable(Arrays.asList(datas).contains("") ? datas.length - 1 : datas.length);
    }
}

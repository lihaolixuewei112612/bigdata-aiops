package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;



/**
 * @author chentong
 */
@Description(
        name = "CountTradeDayNum",
        value = "_FUNC_(IntWritable, IntWritable) "
)

public class CountTradeDayNum extends UDF{
    public IntWritable evaluate(IntWritable integer, IntWritable integer2) {
        try {
            return new IntWritable(TradingDays.getTradingDays(Integer.parseInt(integer.toString()),
                                                              Integer.parseInt(integer2.toString())).size());
        }
        catch (Exception var4)
        {
            return new IntWritable(0);
        }
    }
}


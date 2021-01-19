package com.apex_ids.udfs;

import com.apex_ids.util.DateUtil;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.text.ParseException;
import java.util.Date;

/**
 * @author linqz
 */
@Description(
        name = "DaysBetween",
        value = "_FUNC_(date1,date2) - 返回两个整数型（yyyyMMdd）日期之间的天数 "
)
public class DaysBetween extends UDF {
    private static final Logger log = LoggerFactory.getLogger(DaysBetween.class);

    public IntWritable evaluate(Text dateStr1, Text dateStr2) {
        if (dateStr1 == null || dateStr2 == null) {
            return null;
        }
        try {
            Date date1 = DateUtil.convertStringToDate(dateStr1.toString(), "yyyyMMdd");
            Date date2 = DateUtil.convertStringToDate(dateStr2.toString(), "yyyyMMdd");
            return new IntWritable(DateUtil.getDaysBetween(date1, date2));
        } catch (ParseException e) {
            return null;
        }
    }

    /**
     * tdh下遇到的  前一个参数是decimal类型
     * @param dateStr1
     * @param dateStr2
     * @return
     */
    public IntWritable evaluate(HiveDecimalWritable dateStr1, Text dateStr2) {
        if (dateStr1 == null || dateStr2 == null) {
            return null;
        }
        try {
            Date date1 = DateUtil.convertStringToDate(dateStr1+"", "yyyyMMdd");
            Date date2 = DateUtil.convertStringToDate(dateStr2.toString(), "yyyyMMdd");
            return new IntWritable(DateUtil.getDaysBetween(date1, date2));
        } catch (ParseException e) {
            return null;
        }
    }
    /**
     * tdh下遇到的  前一个参数是decimal类型
     * @param dateStr1
     * @param dateStr2
     * @return
     */
    public IntWritable evaluate(Decimal dateStr1, Text dateStr2) {
        if (dateStr1 == null || dateStr2 == null) {
            return null;
        }
        try {
            Date date1 = DateUtil.convertStringToDate(dateStr1+"", "yyyyMMdd");
            Date date2 = DateUtil.convertStringToDate(dateStr2.toString(), "yyyyMMdd");
            return new IntWritable(DateUtil.getDaysBetween(date1, date2));
        } catch (ParseException e) {
            return null;
        }
    }

    public IntWritable evaluate(DateWritable dw1, DateWritable dw2) {
        if (dw1 == null || dw2 == null) {
            return null;
        }
        return new IntWritable(DateUtil.getDaysBetween(dw1.get(), dw2.get()));
    }


    public IntWritable evaluate(IntWritable sDate, IntWritable eDate) {
        if (sDate == null || eDate == null) {
            return null;
        }
        if (sDate.get() < 10000 || eDate.get() < 10000) {
            return null;
        }
        try {
            Date date1 = DateUtil.convertStringToDate(sDate.get() + "", "yyyyMMdd");
            Date date2 = DateUtil.convertStringToDate(eDate.get() + "", "yyyyMMdd");
            return new IntWritable(DateUtil.getDaysBetween(date1, date2));
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
        return null;
    }

    private IntWritable calcBetweenDays(long sTime, long eTime) {
        IntWritable result = new IntWritable();
        long days = (eTime - sTime) / (1000 * 3600 * 24);
        result.set((int) days);
        return result;
    }
}

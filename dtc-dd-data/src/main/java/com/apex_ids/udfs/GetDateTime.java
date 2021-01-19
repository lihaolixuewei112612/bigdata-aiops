package com.apex_ids.udfs;

import com.apex_ids.util.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;

/**
 * @author linqz
 */
@Description(
        name = "GetDateTime",
        value = "_FUNC_(date,type) - 根据类型返回整数型（yyyyMMdd）日期 "
)
public class GetDateTime extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetDateTime.class);


    private static final String MONTH_FORMAT_STR = "yyyyMM";
    private static final String DAY_FORMAT_STR = "yyyyMMdd";


    public IntWritable evaluate(IntWritable dateStr, IntWritable type) {
        if (dateStr == null || type == null) {
            return null;
        }
        String datetime = dateStr.get() + "";
        Date date;
        try {
            date = DateUtil.convertStringToDate(datetime, DAY_FORMAT_STR);
        } catch (ParseException e) {
            log.error(e.getMessage());
            return null;
        }
        switch (type.get()) {
            case 0:
                //月初（当月第一天）
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getMonthStart(date), DAY_FORMAT_STR)));

            case 1:
                //月末（当月最后一天）
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getMonthEnd(date), DAY_FORMAT_STR)));

            case 2:
                // 月份(如:201001)
                return new IntWritable(Integer.parseInt(datetime.substring(0, datetime.length() - 2)));
            case 3:
                // 月份—上期：
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getLastMonthDay(date), MONTH_FORMAT_STR)));

            case 4:
                //月份—同期
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getLastYearDay(date), MONTH_FORMAT_STR)));

            case 5:
                // 日期—上期
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getLastMonthDay(date), DAY_FORMAT_STR)));

            case 6:
                //日期—同期
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getLastYearDay(date), DAY_FORMAT_STR)));

            case 7:
                // 上月月末
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getLastMonthLastDay(date), DAY_FORMAT_STR)));

            case 8:
                //年(如:2010)
                return new IntWritable(DateUtil.getYearNum(date));
            case 9:
                //季初
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getQuarterStart(date), DAY_FORMAT_STR)));
            case 10:
                //季末
                return new IntWritable(Integer.parseInt(DateUtil.formatDate(DateUtil.getQuarterEnd(date), DAY_FORMAT_STR)));
            default:
                return null;
        }
    }


}

package com.apex_ids.udfs;

import com.apex_ids.util.DateUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;

/**
 * @author linqz
 */
@Description(
        name = "GetDateOffset",
        value = "_FUNC_(date,type,offset,format) - 根据偏移量返回日期时间 "
)
public class GetDateOffset extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetDateOffset.class);

    private static final String R_TYPE = "R";
    private static final String Y_TYPE = "Y";
    private static final String N_TYPE = "N";

    private static final String DEFAULT_FORMAT = "yyyyMMdd";

    public Integer evaluate(Integer date, String type, Integer offset, String format) {
        if (date == null || type == null || offset == null) {
            return null;
        }
        return getDate(date + "", type, offset, format);

    }

    public Integer evaluate(String date, String type, Integer offset, String format) {
        if (date == null || type == null || offset == null) {
            return null;
        }
        return getDate(date, type, offset, format);
    }

    private Integer getDate(String date, String type, Integer offset, String format) {
        Date d;
        String formatStr = DEFAULT_FORMAT;
        try {
            if ("YYYY-MM-DD".equalsIgnoreCase(format)) {
                formatStr = "yyyy-MM-dd";
            } else if ("YYYY/MM/DD".equalsIgnoreCase(format)) {
                formatStr = "yyyy/MM/dd";
            } else if ("YYYY.MM.DD".equalsIgnoreCase(format)) {
                formatStr = "yyyy.MM.dd";
            }
            d = DateUtil.convertStringToDate(date, formatStr);
            if (R_TYPE.equalsIgnoreCase(type)) {
                //自然日
                d = DateUtil.getAfterDay(d, offset);
            }
            if (Y_TYPE.equalsIgnoreCase(type)) {
                //月
                d = DateUtil.getAfterMonth(d, offset);
            }
            if (N_TYPE.equalsIgnoreCase(type)) {
                //年
                d = DateUtil.getAfterYear(d, offset);
            }
        } catch (ParseException e) {
            // parse error
            return null;

        }
        return Integer.parseInt(DateUtil.formatDate(d, "yyyyMMdd"));

    }


}

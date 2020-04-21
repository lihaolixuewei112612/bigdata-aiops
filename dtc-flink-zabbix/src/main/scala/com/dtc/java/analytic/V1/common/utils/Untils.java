package com.dtc.java.analytic.V1.common.utils;

import com.dtc.java.analytic.V1.common.watermarks.DTCPeriodicWatermak;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created on 2019-08-21
 *
 * @author :hao.li
 */
public class Untils {
    private static final Logger LOGGER = LoggerFactory.getLogger(DTCPeriodicWatermak.class);

    public static Long getTime(String str) {
//        String s = str.replaceAll("[A-Z]", " ");
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        Calendar calendar = null;
        try {
            Date date = simpleDateFormat.parse(str);
            calendar = Calendar.getInstance();
            calendar.setTime(date);
            calendar.set(Calendar.HOUR, calendar.get(Calendar.HOUR) + 8);
        } catch (ParseException e) {
            LOGGER.warn("Data parse is mistake,and reasson is {}.", e);
        }
        Date time = calendar.getTime();
        long time1 = time.getTime();
        return time1;
    }



    public static void main(String[] args) throws ParseException {
        String str = "2019-08-26T09:46:28Z";
//        UTCToCST(str, "yyyy-MM-dd'T'HH:mm:ss'Z'");
        Long time = getTime(str);
        System.out.println(time);
    }
}

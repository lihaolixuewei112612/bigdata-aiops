package com.dtc.dingding;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/*"value" -> "["2020-03-07 10:00","2020-03-07 17:00"]"*/
public class test {
    public static void main(String[] args) throws ParseException {
        String start_time = "2020-03-07 21:00";
        String end_time = "2020-03-08 06:33";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm");
        Calendar calendar = Calendar.getInstance();
        try {
            Date date = sdf.parse(start_time);
            calendar.setTime(date);
            calendar.add(Calendar.DATE, 1);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);
        } catch (ParseException e) {
            System.out.println("Date format error!!!!");
        }
        String lc = sdf.format(calendar.getTime());
        Date date_lc = sdf.parse(lc);
        long tlc = date_lc.getTime() / 1000l;
        System.out.println("第2天的时间戳" + tlc);


        //加班时长
        try {
            Date startDate = sdf.parse(start_time);
            Date endDate = sdf.parse(end_time);
            Map timeDifference = convert(startDate, endDate, date_lc);
            System.out.println(timeDifference);
        } catch (ParseException e) {
            e.printStackTrace();
            System.out.println("日期格式化失败");
        }
    }

    public static Map convert(Date startDate, Date endDate, Date date_lc) {
        long startTime = startDate.getTime();//获取毫秒数
        long endTime = endDate.getTime();
        long lcTime = date_lc.getTime();//获取毫秒数
        Map<String, String> map = new HashMap<>();
        String format = null;
        String format1 = null;
        String format2 = null;
        if (endTime <= lcTime) {
            long timeDifference = endTime - startTime;
            double second = timeDifference / 1000;  //计算秒
            double hour = second / 60 / 60;
            DecimalFormat decimalFormat = new DecimalFormat("#.#");
            format = decimalFormat.format(hour);
            map.put("day1", format);
        }
        //加班超过0点 是第二天的加班时间，不过0点是当天的
        else {
            //第一天的加班时间
            long time1 = lcTime - startTime;
            double second1 = time1 / 1000;  //计算秒
            System.out.println(second1);
            double hour1 = second1 / 60 / 60;
            DecimalFormat decimalFormat = new DecimalFormat("#.#");
            format1 = decimalFormat.format(hour1);
            map.put("day01", format1);
            //第二天的加班时间
            long time2 = endTime - lcTime;
            double second2 = time2 / 1000;  //计算秒
            double hour2 = second2 / 60 / 60;
            format2 = decimalFormat.format(hour2);
            map.put("day02", format2);

        }
        return map;

    }

}

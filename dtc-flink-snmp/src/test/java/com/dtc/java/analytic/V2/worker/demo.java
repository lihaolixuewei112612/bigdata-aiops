package com.dtc.java.analytic.V2.worker;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @Author : lihao
 * Created on : 2020-07-22
 * @Description : TODO描述类作用
 */
public class demo {
    public static void main(String[] args) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        Date date = new Date();
        String format1 = format.format(date);
        System.out.println(format1);


        String str = "20_19";
        Integer trim = Integer.parseInt(str.split("_")[0].trim());
        Integer trim1 = Integer.parseInt(str.split("_")[1].trim());
        if (trim1 != 0 && trim < trim1) {
            String time1 = format1 + " " + trim + ":00:00";
            String time2 = format1 + " " + trim + ":00:00";
            System.out.println(time1);
            System.out.println(time2);
        } else if (trim1 == 0 || (trim1 != 0 && trim1 < trim)) {
            String time1 = format1 + " " + trim + ":00:00";
            System.out.println(time1);

            Calendar calendar = new GregorianCalendar();
            calendar.setTime(date);
            calendar.add(calendar.DATE, 1);//把日期往后增加一天.整数往后推,负数往前移动
            date = calendar.getTime(); //这个时间就是日期往后推一天的结果
            String dateString = format.format(date);
            String time2;
            if (trim1.toString().length() == 1) {
                time2 = dateString + " 0" + trim1 + ":00:00";
            } else {
                time2 = dateString + " " + trim1 + ":00:00";
            }
            System.out.println(time2);
        }



    }
}

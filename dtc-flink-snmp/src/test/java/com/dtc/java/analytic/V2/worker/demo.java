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



        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.add(calendar.DATE,1);//把日期往后增加一天.整数往后推,负数往前移动
        date=calendar.getTime(); //这个时间就是日期往后推一天的结果

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        String dateString = formatter.format(date);
        System.out.println(dateString);
        String str ="20_05";
        Integer trim = Integer.parseInt(str.split("_")[0].trim());
        Integer trim1 = Integer.parseInt(str.split("_")[1].trim());
        System.out.println(trim);
        System.out.println(trim1);
    }
}

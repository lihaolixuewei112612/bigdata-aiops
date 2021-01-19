package com.apexsoft.utils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class DateUtils {
    public static String getLastDayOfMaonth(int year, int month){
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month-1);
        int lastDay = cal.getActualMaximum(Calendar.DATE);
        cal.set(Calendar.DAY_OF_MONTH, lastDay);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(cal.getTime());
    }
    public static String getFirstDayOfMonth(int year, int month){
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.MONTH, month-1);
        int firstDay = cal.getMinimum(Calendar.DATE);
        cal.set(Calendar.DAY_OF_MONTH, firstDay);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        return sdf.format(cal.getTime());
    }
    public static List<Integer> getMonths(int begin, int end, boolean includeUncompete){
        List<Integer> months = new ArrayList<>();
        try{
            String beginMonth = String.valueOf(begin).substring(0, 6);
            String endMonth = String.valueOf(end).substring(0, 6);
            boolean compete = isLastMonthDay(end);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
            Date min = sdf.parse(beginMonth);
            Date max = sdf.parse(endMonth);
            Calendar cal = Calendar.getInstance();
            cal.setTime(min);
            while (cal.getTime().before(max)){
                months.add(Integer.valueOf(sdf.format(cal.getTime())));
                cal.add(Calendar.MONTH, 1);
            }
            if (includeUncompete || compete){
                months.add(Integer.valueOf(sdf.format(cal.getTime())));
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return months;
    }
    public static boolean isLastMonthDay(int day){
        int year = Integer.valueOf(String.valueOf(day).substring(0, 4));
        int month = Integer.valueOf(String.valueOf(day).substring(4, 6));
        String lastDay = getLastDayOfMaonth(year, month);
        if (Integer.valueOf(lastDay).intValue() == day){
            return true;
        }else{
            return false;
        }
    }
    public static int getYear(int day){
        return Integer.valueOf(String.valueOf(day).substring(0, 4));
    }
    public static int getMonth(int day){
        return Integer.valueOf(String.valueOf(day).substring(4, 6));
    }

    public static int getNextMonth(int day, int shift) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = parseDay(day, sdf);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, 1);
        cal.add(Calendar.DAY_OF_MONTH, shift);
        return Integer.valueOf(sdf.format(cal.getTime()));

    }
    private static Date parseDay(int day, SimpleDateFormat sdf){
        Date date = null;
        try{
            date = sdf.parse(String.valueOf(day));
        }catch (Exception e){
            return new Date();
        }

        return date;
    }
    public static int getNextDay(int day, int yearShift, int monthShift, int dayShift){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = parseDay(day, sdf);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.YEAR, yearShift);
        cal.add(Calendar.MONTH, monthShift);
        cal.add(Calendar.DAY_OF_YEAR, dayShift);
        return Integer.valueOf(sdf.format(cal.getTime()));
    }
    /**
     * 获取两个日期的时间差
     *
     * @param start
     * @param end
     * @return
     */
    public static int getDaysBetween(int start, int end){
        int days = 0;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date startDate = parseDay(start, sdf);
        Date endDate = parseDay(end, sdf);
        Calendar startCalendar = Calendar.getInstance();
        startCalendar.setTime(startDate);
        startCalendar.set(Calendar.HOUR_OF_DAY, 0);
        startCalendar.set(Calendar.MINUTE, 0);
        startCalendar.set(Calendar.SECOND, 0);

        Calendar endCalendar = Calendar.getInstance();
        endCalendar.setTime(endDate);
        endCalendar.set(Calendar.HOUR_OF_DAY, 0);
        endCalendar.set(Calendar.MINUTE, 0);
        endCalendar.set(Calendar.SECOND, 0);

        if (startCalendar.after(endCalendar)) {
            while (startCalendar.after(endCalendar)) {
                days--;
                startCalendar.roll(Calendar.DAY_OF_YEAR, 1);
            }
            return days;
        } else {
            while (startCalendar.before(endCalendar)) {
                days++;
                startCalendar.add(Calendar.DAY_OF_YEAR, 1);
            }
            return days;
        }
    }
}

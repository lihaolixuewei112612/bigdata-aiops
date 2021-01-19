package com.apex_ids.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

/**
 * @author 施均刚
 * @version 1.0
 */
public class DateUtil {
    /**
     * 根据传入的模式参数返回当天的日期
     *
     * @param pattern 传入的模式
     * @return 按传入的模式返回一个字符串
     */
    public static String getToday(String pattern) {
        Date date = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 比较两个日期字符串的大小
     *
     * @param date1    日期字符串
     * @param pattern1 日期格式
     * @param date2    日期字符串
     * @param pattern2 日期格式
     * @return boolean 若是date1比date2小则返回true
     * @throws ParseException
     */
    public static boolean compareMinDate(String date1, String pattern1, String date2, String pattern2)
            throws ParseException {
        Date d1 = convertToCalendar(date1, pattern1).getTime();
        Date d2 = convertToCalendar(date2, pattern2).getTime();
        return d1.before(d2);
    }

    /**
     * 比较两个日期大小
     *
     * @param date1 Date
     * @param date2 Date
     * @return boolean 若是date1比date2小则返回true
     */
    public static boolean compareMinDate(Date date1, Date date2) {
        try {
            return DateUtil.compareMinDate(DateUtil.formatDate(date1, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss",
                    DateUtil.formatDate(date2, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss");
        } catch (Exception ex) {
            return false;
        }
    }

    /**
     * 根据传入的日期字符串以及格式，产生一个Calendar对象
     *
     * @param date    日期字符串
     * @param pattern 日期格式
     * @return Calendar
     * @throws ParseException 当格式与日期字符串不匹配时抛出该异常
     */
    public static Calendar convertToCalendar(String date, String pattern) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Date d = sdf.parse(date);
        GregorianCalendar calendar = new GregorianCalendar();
        calendar.setTime(d);
        return calendar;
    }

    /**
     * 按指定的格式，格式化日期字符串
     *
     * @param pattern     字符串的格式
     * @param currentDate 被格式化日期
     * @return String 已格式化的日期字符串
     * @throws NullPointerException 如果参数为空
     */
    public static String formatDate(Calendar currentDate, String pattern) {
        if (currentDate == null || pattern == null) {
            throw new NullPointerException("The arguments are null !");
        }
        Date date = currentDate.getTime();
        return formatDate(date, pattern);
    }

    /**
     * 将指定格式的时间String转为Date类型
     *
     * @param dateStr String 待转换的时间String
     *
     * @return Date
     * @throws ParseException
     */
    public static Date convertStringToDate(String dateStr, String patternner) throws ParseException {
        String pattern = patternner;

        if (StringUtil.isEmpty(dateStr)) {
            return null;
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(dateStr);
    }

    /**
     * 用途：以指定的格式格式化日期字符串
     *
     * @param pattern 字符串的格式
     * @param date    被格式化日期
     * @return String 已格式化的日期字符串
     * @throws NullPointerException 如果参数为空
     */
    public static String formatDate(Date date, String pattern) {
        if (date == null || pattern == null) {
            throw new NullPointerException("The arguments are null !");
        }
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 用途：以指定的格式格式化日期字符串
     *
     * @param currentDate 被格式化日期字符串 必须为yyyymmdd
     * @param pattern     字符串的格式
     * @return String 已格式化的日期字符串
     * @throws NullPointerException 如果参数为空
     * @throws ParseException       若被格式化日期字符串不是yyyymmdd形式时抛出
     */
    public static String formatDate(String currentDate, String pattern) throws ParseException {
        if (currentDate == null || pattern == null) {
            throw new NullPointerException("The arguments are null !");
        }
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Date date = sdf.parse(currentDate);
        sdf.applyPattern(pattern);
        return sdf.format(date);
    }

    /**
     * 用途：以指定的格式格式化日期字符串
     *
     * @param strDate  被格式化日期字符串 必须为yyyymmdd
     * @param formator 格式字符串
     * @return String 已格式化的日期字符串
     * @throws NullPointerException 如果参数为空
     * @throws ParseException       若被格式化日期字符串不是yyyymmdd形式时抛出
     */
    public static Calendar strToDate(String strDate, String formator) {
        if (strDate == null || formator == null) {
            throw new NullPointerException("The arguments are null !");
        }

        Calendar date = Calendar.getInstance();
        date.setTime(java.sql.Date.valueOf(strDate));
        return date;
    }

    /**
     * 判断当前时间是否在参数时间内（当开始时间大于结束时间表示时间段的划分从begin到第二天的end时刻） 例如当前时间在12：00
     * 传入参数为（12,12,0,1）返回true 例如当前时间在12：00 传入参数为（12,12,1,0）返回true
     *
     * @param beginHour int 开始的小时值
     * @param endHour   int 结束的小时值
     * @param beginMinu int 开始的分钟值
     * @param endMinu   int 结束的分钟值
     * @return boolean
     */
    @SuppressWarnings("deprecation")
    public static boolean isInTime(int beginHour, int endHour, int beginMinu, int endMinu) {
        Date date1 = new Date();
        Date date2 = new Date();
        Date nowDate = new Date();
        date1.setHours(beginHour);
        date2.setHours(endHour);
        date1.setMinutes(beginMinu);
        date2.setMinutes(endMinu);
        if (date1 == date2) {
            return false;
        }
        // yyyy-MM-dd HH:mm:ss
        if (DateUtil.compare(date2, date1)) {
            if (!DateUtil.compare(nowDate, date1) || DateUtil.compare(nowDate, date2)) {
                return true;
            }
        } else {
            if (!DateUtil.compare(nowDate, date1) && DateUtil.compare(nowDate, date2)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 按指定格式获取本月起始时间，如2011-12-1 00:00:01
     *
     * @return
     */
    public static Date getCurrentMonthStart() {
        Calendar cal = Calendar.getInstance();
        return getMonthStart(cal);

    }

    /**
     * 按指定格式获取本月结束时间，如2011-12-31 23:59:59
     *
     * @return
     */
    public static Date getCurrentMonthEnd() {
        Calendar cal = Calendar.getInstance();
        return getMonthEnd(cal);
    }

    /**
     * 根据给定Calendar给出当月初始时间
     *
     * @param cal
     * @return
     */
    public static Date getMonthStart(Calendar cal) {
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMinimum(Calendar.DAY_OF_MONTH), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 根据给定Date给出当月初始时间
     *
     * @param date
     * @return
     */
    public static Date getMonthStart(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMinimum(Calendar.DAY_OF_MONTH), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
    }

    /**
     * 根据给定Calendar给出当月结束时间
     *
     * @param cal
     * @return
     */
    public static Date getMonthEnd(Calendar cal) {
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMaximum(Calendar.DAY_OF_MONTH), 23, 59, 59);
        return cal.getTime();

    }

    /**
     * 根据给定Date给出当月结束时间
     *
     * @param date
     * @return
     */
    public static Date getMonthEnd(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMaximum(Calendar.DAY_OF_MONTH), 23, 59, 59);
        return cal.getTime();

    }

    /**
     * 获取该天开始时间
     *
     * @param date
     * @return
     */
    public static Date getDateStart(Date date) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 0, 0, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    /**
     * 获取该天结束时间
     *
     * @param date
     * @return
     */
    public static Date getDateEnd(Date date) {
        if (date == null) {
            return null;
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
        cal.set(Calendar.MILLISECOND, 999);
        return cal.getTime();
    }

    /**
     * 开始时间小于结束时间返回true，否则返回false
     *
     * @param beginDate Date
     * @param endDate   Date
     * @return boolean
     */
    private static boolean compare(Date beginDate, Date endDate) {
        try {

            return DateUtil.compareMinDate(DateUtil.formatDate(beginDate, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss",
                    DateUtil.formatDate(endDate, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss");

        } catch (Exception ex) {
            // log.error ( "时间格式转换错误" + ex ) ;
            return false;
        }
    }

    /**
     * 判断时间是否符合表达式
     *
     * @param date
     * @param pattern
     * @return
     */
    public static boolean checkDatePattern(String date, String pattern) {
        if (date == null || pattern == null) {
            throw new NullPointerException("The arguments are null !");
        }
        try {
            DateUtil.convertStringToDate(date, pattern);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 获取时间差
     *
     * @param start
     * @param end
     * @return
     */
    public static String lastTime(Date start, Date end) {
        if (start != null && end != null) {
            long l = end.getTime() - start.getTime();
            long day = l / (24 * 60 * 60 * 1000);
            long hour = (l / (60 * 60 * 1000) - day * 24);
            long min = ((l / (60 * 1000)) - day * 24 * 60 - hour * 60);
            long s = (l / 1000 - day * 24 * 60 * 60 - hour * 60 * 60 - min * 60);
            StringBuffer returnDate = new StringBuffer("");
            if (day != 0) {
                returnDate.append(day + "天");
            }
            if (hour != 0) {
                returnDate.append(hour + "小时");
            }
            if (min != 0) {
                returnDate.append(min + "分");
            }
            if (s != 0) {
                returnDate.append(s + "秒");
            }
            if (l < 1000) {
                returnDate.append(l + "毫秒");
            }
            return returnDate.toString();
        }
        return "";
    }

    /**
     * 获取某年的某星期的开始时间
     *
     * @param year
     * @param weekNum
     * @return
     */
    public static Date getWeekStart(int year, int weekNum) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.WEEK_OF_YEAR, weekNum);
        calendar.set(Calendar.DAY_OF_WEEK, 2);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);

        return calendar.getTime();
    }

    /**
     * 获取该天的周数
     *
     * @param date
     * @return
     */
    public static int getWeekNum(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.WEEK_OF_YEAR);
    }

    /**
     * 获取该天的年份
     *
     * @param date
     * @return
     */
    public static int getYearNum(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    public static Date getYesterday() {
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE, -1);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTime();
    }

    /**
     * 获取某年的某星期的结束时间
     *
     * @param year
     * @param weekNum
     * @return
     */
    public static Date getWeekEnd(int year, int weekNum) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.WEEK_OF_YEAR, weekNum);
        calendar.set(Calendar.DAY_OF_WEEK, 7);
        calendar.add(Calendar.DATE, 1);
        calendar.set(Calendar.HOUR_OF_DAY, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        return calendar.getTime();
    }

    /**
     * 获取季度开始时间
     *
     * @param year
     * @param quarterly
     * @return
     * @throws ParseException
     */
    public static Date getQuarterlyStart(int year, int quarterly) throws ParseException {
        if (quarterly == 1) {
            return convertStringToDate(year + "-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 2) {
            return convertStringToDate(year + "-04-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 3) {
            return convertStringToDate(year + "-07-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 4) {
            return convertStringToDate(year + "-10-01 00:00:00", "yyyy-MM-dd HH:mm:ss");
        }
        return null;
    }

    /**
     * 获取季度结束时间
     *
     * @param year
     * @param quarterly
     * @return
     * @throws ParseException
     */
    public static Date getQuarterlyEnd(int year, int quarterly) throws ParseException {
        if (quarterly == 1) {
            return convertStringToDate(year + "-03-31 23:59:59", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 2) {
            return convertStringToDate(year + "-06-30 23:59:59", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 3) {
            return convertStringToDate(year + "-09-30 23:59:59", "yyyy-MM-dd HH:mm:ss");
        }
        if (quarterly == 4) {
            return convertStringToDate(year + "-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss");
        }
        return null;
    }

    /**
     * 获取季度数
     *
     * @param date
     * @return
     */
    public static int getQuarterlyNum(Date date) {
        Calendar c = Calendar.getInstance();
        c.setTime(date);
        int currentMonth = c.get(Calendar.MONTH) + 1;
        if (currentMonth >= 1 && currentMonth <= 3) {
            return 1;
        } else if (currentMonth >= 4 && currentMonth <= 6) {
            return 2;
        } else if (currentMonth >= 7 && currentMonth <= 9) {
            return 3;
        } else if (currentMonth >= 10 && currentMonth <= 12) {
            return 4;
        }
        return 0;
    }

    /**
     * 得到指定月的天数
     *
     * @param year
     * @param month
     * @return
     */
    public static int getMonthLastDay(int year, int month) {
        Calendar a = Calendar.getInstance();
        a.set(Calendar.YEAR, year);
        a.set(Calendar.MONTH, month - 1);
        a.set(Calendar.DATE, 1);// 把日期设置为当月第一天
        a.roll(Calendar.DATE, -1);// 日期回滚一天，也就是最后一天
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }

    /**
     * 得到指定月的天数
     *
     * @param date
     * @return
     */
    public static int getMonthLastDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.set(Calendar.DATE, 1);// 把日期设置为当月第一天
        a.roll(Calendar.DATE, -1);// 日期回滚一天，也就是最后一天
        int maxDate = a.get(Calendar.DATE);
        return maxDate;
    }

    /**
     * 获取第二天
     *
     * @param date
     * @return
     */
    public static Date getNextDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.DATE, 1);
        return a.getTime();
    }

    /**
     * 获取第二天的开始时间
     *
     * @param date
     * @return
     */
    public static Date getNextDayStart(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.DATE, 1);
        a.set(Calendar.HOUR_OF_DAY, 0);
        a.set(Calendar.MINUTE, 0);
        a.set(Calendar.SECOND, 0);
        return a.getTime();
    }

    /**
     * 获取第二天的结束时间
     *
     * @param date
     * @return
     */
    public static Date getNextDayEnd(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.DATE, 1);
        a.set(Calendar.HOUR_OF_DAY, 23);
        a.set(Calendar.MINUTE, 59);
        a.set(Calendar.SECOND, 59);
        return a.getTime();
    }

    /**
     * 获取下个月的今天
     *
     * @param date
     * @return
     */
    public static Date getNextMonthDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.MONTH, 1);
        return a.getTime();
    }

    public static Date getLastMonthDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.MONTH, -1);
        return a.getTime();
    }

    public static Date getLastMonthLastDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MONTH, -1);
        cal.set(cal.get(Calendar.YEAR), cal.get(Calendar.MONTH), cal.getActualMaximum(Calendar.DAY_OF_MONTH), 23, 59, 59);

        return cal.getTime();
    }

    /**
     * 获取下个月的第一天
     *
     * @return
     */
    public static Date getNextMonthFirstDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.MONTH, 1);
        a.set(Calendar.DAY_OF_MONTH, 1);
        a.set(Calendar.HOUR_OF_DAY, 0);
        a.set(Calendar.MINUTE, 0);
        a.set(Calendar.SECOND, 0);
        return a.getTime();
    }

    /**
     * 获取两个日期的时间差
     *
     * @param startDate
     * @param endDate
     * @return
     */
    public static int getDaysBetween(Date startDate, Date endDate) {
        int days = 0;
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
                //startCalendar.roll(Calendar.DAY_OF_YEAR, 1);
                startCalendar.add(Calendar.DAY_OF_YEAR, -1);
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

    public static String getCurrentWeek() {
        int year = Integer.parseInt(DateUtil.formatDate(new Date(), "yyyy"));
        int weekNum = DateUtil.getWeekNum(new Date()) - 1;
        String weekStr = weekNum + "周(" + DateUtil.formatDate(DateUtil.getWeekStart(year, weekNum), "yyyy-MM-dd") + "至"
                + DateUtil.formatDate(DateUtil.getWeekEnd(year, weekNum), "yyyy-MM-dd") + ")";
        return weekStr;
    }

    /**
     * 获取date的n天偏移
     *
     * @param date
     * @param dayDiv
     * @return
     */
    public static Date getAfterDay(Date date, int dayDiv) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.DATE, dayDiv);
        return a.getTime();
    }

    /**
     * 获取date的n年偏移
     *
     * @param date
     * @param dayDiv
     * @return
     */
    public static Date getAfterYear(Date date, int dayDiv) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.YEAR, dayDiv);
        return a.getTime();
    }

    /**
     * 获取date的n月偏移
     *
     * @param date
     * @param dayDiv
     * @return
     */
    public static Date getAfterMonth(Date date, int dayDiv) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.MONTH, dayDiv);
        return a.getTime();
    }

    /**
     * 获取明年的现在
     *
     * @param date
     * @return
     */
    public static Date getNextYearDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.YEAR, 1);
        return a.getTime();
    }

    /**
     * 获取去年的现在
     *
     * @param date
     * @return
     */
    public static Date getLastYearDay(Date date) {
        Calendar a = Calendar.getInstance();
        a.setTime(date);
        a.add(Calendar.YEAR, -1);
        return a.getTime();
    }

    /**
     * 根据时间字符串来获取时间
     *
     * @param time
     * @return
     */
    public static Date getByTimeStr(String time) {
        String[] timeArr = time.split(":");
        Date date = new Date();// 取时间
        Calendar calendar = new GregorianCalendar();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, Integer.parseInt(timeArr[0]));
        calendar.set(Calendar.MINUTE, Integer.parseInt(timeArr[1]));
        calendar.set(Calendar.SECOND, Integer.parseInt(timeArr[2]));
        date = calendar.getTime(); // 这个时间就是日期往后推一天的结果
        if (date.before(new Date())) {
            calendar.add(Calendar.DATE, 1);// 把日期往后增加一天.整数往后推,负数往前移动
        }
        return calendar.getTime();
    }

    /**
     * 获取当前时间的周一
     *
     * @return
     */
    public static Date getNowWeekBegin() {
        int mondayPlus;
        Calendar cd = Calendar.getInstance();
        int dayOfWeek = cd.get(Calendar.DAY_OF_WEEK) - 1;
        if (dayOfWeek == 1) {
            mondayPlus = 0;
        } else {
            mondayPlus = 1 - dayOfWeek;
        }
        GregorianCalendar currentDate = new GregorianCalendar();
        currentDate.add(GregorianCalendar.DATE, mondayPlus);
        return currentDate.getTime();
    }

    /**
     * 获取时间类型 am pm
     *
     * @param time 格式 08:00
     * @author linqz
     * 11/4/2016 4:57 PM
     */
    public static String getTimeType(String time) {
        if (time == null || time.equals("")) return "";
        int var2 = Integer.parseInt(time.split(":")[0]);
        return var2 >= 0 && var2 < 12 ? "AM" : "PM";
    }

    // 季度一年四季， 第一季度：1月-3月， 第二季度：4月-6月， 第三季度：7月-9月， 第四季度：10月-12月
    private static int getQuarterInMonth(int month, boolean isQuarterStart) {
        int months[] = { 1, 4, 7, 10 };
        if (!isQuarterStart) {
            months = new int[] { 3, 6, 9, 12 };
        }
        if (month >= 1 && month <= 3)
            return months[0];
        else if (month >= 4 && month <= 6)
            return months[1];
        else if (month >= 7 && month <= 9)
            return months[2];
        else
            return months[3];
    }

    /**
     * 获取季初
     * @param date 日期
     * @return
     */
    public static Date getQuarterStart(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int month = getQuarterInMonth(calendar.get(Calendar.MONTH) + 1, true);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        return calendar.getTime();

    }

    /**
     * 获取季末
     * @param date 日期
     * @return
     */
    public static Date getQuarterEnd(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int month = getQuarterInMonth(calendar.get(Calendar.MONTH) + 1, false);
        calendar.set(Calendar.MONTH, month - 1);
        calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
        return calendar.getTime();
    }
}

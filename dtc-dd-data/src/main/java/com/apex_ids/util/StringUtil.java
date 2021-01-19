package com.apex_ids.util;


import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class StringUtil {

    /**
     * 获取当前时间格式化后的字符串,模式为'yyyy-MM-dd HH:mm:ss'
     *
     * @return
     */
    public static String getDateFormateString() {
        Calendar cal = Calendar.getInstance(Locale.CHINESE);
        return getDateFormatString(cal.getTime(), "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 获取日期格式化后字符串
     *
     * @param date    时间
     * @param pattern 时间模式
     * @return
     */
    public static String getDateFormatString(Date date, String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.format(date);
    }

    /**
     * 是否是符合长度的数字或字母组合
     *
     * @param str    字符串
     * @param length 符合的长度
     * @return
     */
    public static boolean isLetterOrNumber(String str, int length) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        str = str.trim();
        if (str.length() < length) {
            return false;
        }
        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (!((tmp >= 48 && tmp <= 57) || (tmp >= 65 && tmp <= 90) || (tmp >= 97 && tmp <= 122))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 是否数字或字母组合
     *
     * @param str 字符串
     * @return
     */
    public static boolean isLetterOrNumber(String str) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        str = str.trim();

        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (!((tmp >= 48 && tmp <= 57) || (tmp >= 65 && tmp <= 90) || (tmp >= 97 && tmp <= 122))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 是否只为字母
     *
     * @param str
     * @return
     */
    public static boolean isLetter(String str) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        str = str.trim();

        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (!((tmp >= 65 && tmp <= 90) || (tmp >= 97 && tmp <= 122))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断字符串是否为数字串
     *
     * @param str
     * @param minLength 数字最小长度
     * @return
     */
    public static boolean isNumber(String str, int minLength) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        str = str.trim();
        if (str.length() < minLength) {
            return false;
        }
        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (!((tmp >= 48 && tmp <= 57))) {
                return false;
            }
        }
        return true;
    }

    /**
     * 过滤掉单双引号
     *
     * @param str
     * @return
     */
    public static boolean isLegalityStr(String str) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (tmp == 34 || tmp == 39) {
                return false;
            }
        }
        return true;
    }

    /**
     * 是否是符合的号码
     *
     * @param num
     * @return
     */
    public static boolean isPhoneNumber(String num) {
        if (num == null || num.trim().length() == 0) {
            return false;
        }
        char tmp;
        for (int i = 0; i < num.length(); i++) {
            tmp = num.charAt(i);
            if (!((tmp >= 48 && tmp <= 57) || (tmp == 45))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isEmpty(String str) {
        if (str == null || str.equals("")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean notEmpty(String str) {
        if (str != null && str.length() > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 检查字符串是否为字母、数字或汉字
     *
     * @param str    要检查字符串
     * @param minLen 最小长度
     * @param maxLen 最大长度
     * @return
     */
    public static boolean isLetterOrNumOrChinese(String str, int minLen, int maxLen) {
        String reg = "[a-zA-Z0-9\u4E00-\u9FFF]{" + minLen + "," + maxLen + "}";
        return str.trim().matches(reg);
    }

    /**
     * 判断是否为指定的时间格式
     *
     * @param str
     * @param pattern
     * @return
     */
    public static boolean isDate(String str, String pattern) {
        try {
            DateUtil.convertStringToDate(str, pattern);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 判断是否为金额
     *
     * @param str
     * @return
     */
    public static boolean isPrice(String str) {
        String reg = "(([1-9]\\d*)|\\d)((\\.\\d{1,2})|)";
        return str.matches(reg);
    }

    /**
     * 格式化输出金额
     *
     * @param d
     * @return
     */
    public static String getFormatAmount(Double d) {
        if (d == null) {
            return "0.00";
        }
        DecimalFormat df = new DecimalFormat("0.00");
        return df.format(d);
    }

    /**
     * 格式化输出金额
     *
     * @param i
     * @return
     */
    public static String getFormatAmount(int i) {
        DecimalFormat df = new DecimalFormat("0.00");
        return df.format(i);
    }

    /**
     * 将第一个字符转为大写
     *
     * @param str
     * @return
     */
    public static String toFirstUpperCase(String str) {
        return Character.toUpperCase(str.charAt(0)) + str.substring(1);
    }

    /**
     * 格式化数字
     *
     * @param number
     * @param pattern
     * @return
     */
    public static String formatNumber(Double number, String pattern) {
        DecimalFormat df = new DecimalFormat(pattern);
        return df.format(number);
    }

    /**
     * 格式化数字
     *
     * @param number
     * @param pattern
     * @return
     */
    public static String formatNumber(Float number, String pattern) {
        DecimalFormat df = new DecimalFormat(pattern);
        return df.format(number);
    }

    /**
     * 格式化数字
     *
     * @param number
     * @param pattern
     * @return
     */
    public static String formatNumber(Integer number, String pattern) {
        DecimalFormat df = new DecimalFormat(pattern);
        return df.format(number);
    }

    /**
     * 补齐字符串
     *
     * @param str
     * @param filledChar
     * @param totalLen
     * @return
     */
    public static String fillStringLen(String str, String filledChar, int totalLen) {
        int i = totalLen - str.length();
        StringBuilder sb = new StringBuilder();
        while (i-- > 0) {
            sb.append(filledChar);
        }
        sb.append(str);
        return sb.toString();
    }

    /**
     * 补齐字符串
     *
     * @param str
     * @param filledChar
     * @param totalLen
     * @return
     */
    public static String fillStringLenBack(String str, String filledChar, int totalLen) {
        if (str == null) {
            str = "";
        }
        int i = totalLen - str.length();
        StringBuilder sb = new StringBuilder(str);
        while (i-- > 0) {
            sb.append(filledChar);
        }
        return sb.toString();
    }

    // 判断是否为卡号
    public static boolean isCardNo(String str) {
        Pattern pattern = Pattern.compile("([0-9]){4,19}");
        return pattern.matcher(str).matches();
    }

    // 返回输入的中文+其他字母的长度
    public static int isLengthByCh(String str) {
        String reg = "/^[\u4E00-\u9FA5]$/";
        int len = 0;
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            if (String.valueOf(c).matches(reg)) {
                len += 2;
            } else {
                len += 1;
            }
        }
        return len;
    }

    public static boolean isEmpty2(String str) {
        if (str == null || str.trim().equals("")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 判断字符串是否为数字串
     *
     * @param str
     * @return
     */
    public static boolean isNumber(String str) {
        if (str == null || str.trim().length() == 0) {
            return false;
        }
        str = str.trim();
        char tmp;
        for (int i = 0; i < str.length(); i++) {
            tmp = str.charAt(i);
            if (!((tmp >= 48 && tmp <= 57))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isTel(String str) {
        String reg = "0\\d{2,3}-\\d{7,8}";
        return str.trim().matches(reg);
    }

    public static boolean isDecimal(String str) {
        if (str == null || "".equals(str))
            return false;
        Pattern pattern = Pattern.compile("[0-9]*(\\.?)[0-9]*");
        return pattern.matcher(str).matches();
    }

    /**
     * 过滤空格
     *
     * @param str
     * @return
     */
    public static String splitSpace(String str) {
        String regEx = "[ ]";
        Pattern p = Pattern.compile(regEx);
        Matcher m = p.matcher(str);
        return m.replaceAll("").trim();
    }

    /**
     * 将string转为integer数组
     *
     * @param str
     * @return
     */
    public static Integer[] parseToIntegerArray(String str) {
        if (isEmpty(str)) {
            return null;
        }
        String[] tmp = str.split(",");
        Integer[] result = new Integer[tmp.length];
        for (int i = 0; i < tmp.length; i++) {
            result[i] = Integer.parseInt(tmp[i].trim());
        }
        return result;
    }

    /**
     * 将string转为integer队列
     *
     * @param str
     * @return
     */
    public static List<Integer> parseToIntegerList(String str) {
        if (isEmpty(str)) {
            return null;
        }
        String[] tmp = str.split(",");
        List<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < tmp.length; i++) {
            result.add(Integer.parseInt(tmp[i]));
        }
        return result;
    }

    /**
     * 将string转为String队列
     *
     * @param str
     * @return
     */
    public static List<String> parseToStringList(String str) {
        if (isEmpty(str)) {
            return null;
        }
        String[] tmp = str.split(",");
        List<String> result = new ArrayList<String>();
        for (int i = 0; i < tmp.length; i++) {
            result.add(tmp[i]);
        }
        return result;
    }

    /**
     * 判断是否为数字、逗号或者减号的组合 是:返回true 否:返回false
     */

    public static boolean isNumAndDG(String str) {
        String reg = "^[0-9\\,\\-]+$";
        return str.matches(reg);
    }

    /**
     * 判断是否为字母或者点号的组合 是:返回true 否:返回false
     */
    public static boolean isNumAndD(String str) {
        String reg = "^[A-Za-z\\.]+$";
        return str.matches(reg);
    }

    public static boolean isInt(String str) {
        try {
            Integer.valueOf(str);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // 去除后缀
    public static String removeSuffix(String sessionId) {
        if (sessionId != null) {
            int index = sessionId.indexOf(".");
            if (index != -1) {
                sessionId = sessionId.substring(0, index);
            }
        }
        return sessionId;
    }

    /**
     * 功能描述：判断身份证号码是否正确
     *
     * @param idCard
     * @return
     * @throws ParseException
     * @author luoxh
     * @date: 2014-11-21 下午4:38:46
     */
    public static String idCardValidate(String idCard) throws ParseException {
        String errorInfo = "";// 记录错误信息
        String[] ValCodeArr = {"1", "0", "x", "9", "8", "7", "6", "5", "4",
                "3", "2"};
        String[] Wi = {"7", "9", "10", "5", "8", "4", "2", "1", "6", "3", "7",
                "9", "10", "5", "8", "4", "2"};
        String Ai = "";
        if (idCard.length() != 15 && idCard.length() != 18) {
            errorInfo = "身份证号码无效";
//        errorInfo = "身份证号码长度应该为15位或18位。";
            return errorInfo;
        }
        if (idCard.length() == 18) {
            Ai = idCard.substring(0, 17);
        } else if (idCard.length() == 15) {
            Ai = idCard.substring(0, 6) + "19" + idCard.substring(6, 15);
        }
        if (isNumber(Ai) == false) {
            errorInfo = "身份证号码无效";
//    	errorInfo = "身份证15位号码都应为数字 ; 18位号码除最后一位外，都应为数字。";
            return errorInfo;
        }
        //出生年月是否有效
        String strYear = Ai.substring(6, 10);// 年份
        String strMonth = Ai.substring(10, 12);// 月份
        String strDay = Ai.substring(12, 14);// 月份
        if (isDate(strYear + "-" + strMonth + "-" + strDay, "yyyy-MM-dd") == false) {
            errorInfo = "身份证号码无效";
//        errorInfo = "身份证生日无效。";
            return errorInfo;
        }
        GregorianCalendar gc = new GregorianCalendar();
        SimpleDateFormat s = new SimpleDateFormat("yyyy-MM-dd");
        if ((gc.get(Calendar.YEAR) - Integer.parseInt(strYear)) > 150
                || (gc.getTime().getTime() - s.parse(
                strYear + "-" + strMonth + "-" + strDay).getTime()) < 0) {
            errorInfo = "身份证号码无效";
//        errorInfo = "身份证生日不在有效范围。";
            return errorInfo;
        }
        if (Integer.parseInt(strMonth) > 12 || Integer.parseInt(strMonth) == 0) {
            errorInfo = "身份证号码无效";
//      errorInfo = "身份证月份无效";
            return errorInfo;
        }
        if (Integer.parseInt(strDay) > 31 || Integer.parseInt(strDay) == 0) {
            errorInfo = "身份证号码无效";
//      errorInfo = "身份证日期无效";
            return errorInfo;
        }
        //判断最后一位的值
        int TotalmulAiWi = 0;
        for (int i = 0; i < 17; i++) {
            TotalmulAiWi = TotalmulAiWi
                    + Integer.parseInt(String.valueOf(Ai.charAt(i)))
                    * Integer.parseInt(Wi[i]);
        }
        int modValue = TotalmulAiWi % 11;
        String strVerifyCode = ValCodeArr[modValue];
        Ai = Ai + strVerifyCode;
        if (idCard.length() == 18) {
            if (Ai.equalsIgnoreCase(idCard) == false) {
                errorInfo = "身份证号码无效";
                return errorInfo;
            }
        } else {
            return "";
        }
        return "";
    }

    /**
     * 功能描述：通过身份证获取生日
     *
     * @param idCard
     * @return
     * @author luoxh
     * @date: 2014-11-22 下午3:32:11
     */
    public static Date getBirthdayByIDCard(String idCard) {
        String idCardCopy = idCard;
        //先对身份证号码进行验证
        String checkMessage = "";
        try {
            checkMessage = StringUtil.idCardValidate(idCardCopy);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        if (checkMessage == null || "".equals(checkMessage)) {
            //验证通过，提取生日信息
            if (idCard.length() == 15) {
                idCardCopy = idCard.substring(0, 6) + "19" + idCard.substring(6, 15);
            }
            String birthday = idCardCopy.substring(6, 14);
            try {
                Date birthDay = new SimpleDateFormat("yyyyMMdd").parse(birthday);
                return birthDay;
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * 功能描述：验证邮箱
     *
     * @param email
     * @return
     * @author luoxh
     * @date: 2014-11-22 下午3:31:11
     */
    public static boolean isEmail(String email) {
        boolean tag = true;
        String reg = "^([a-z0-9A-Z]+[-|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
        Pattern pattern = Pattern.compile(reg);
        Matcher mat = pattern.matcher(email);
        if (!mat.find()) {
            tag = false;
        }
        return tag;
    }

    /**
     * 手机号字符串中间n位用*号代替
     *
     * @param org
     * @param n
     * @return
     */
    public static String replacePhoneString(String org, int n) {
        char[] cs = org.toCharArray();
        for (int i = 0; i < n; i++) {
            cs[cs.length - i - 5] = '*';
        }
        return String.valueOf(cs);
    }

    /**
     * 隐藏名字用*号代替
     *
     * @param name
     * @return
     */
    public static String replaceNameString(String name) {
        if (isEmpty2(name)) {
            return "";
        }
        StringBuilder nameSb = new StringBuilder();
        if (name.length() <= 1) {
            nameSb.append(name);
        } else if (name.length() <= 2) {
            nameSb.append("*").append(name.charAt(1));
        } else {
            for (int i = 0; i < name.length() - 2; i++) {
                nameSb.append("*");
            }
            nameSb.append(name.substring(name.length() - 2));
        }
        return nameSb.toString();
    }

    /**
     * 截取流水号后N位
     *
     * @param number
     * @param useLength
     * @return
     */
    public static String getNumberSubString(String number, int useLength) {
        if (isEmpty2(number)) {
            return "";
        }
        int orderNumberLengthDiffer = number.length() - useLength;
        if (orderNumberLengthDiffer > 0) {
            number = number.substring(orderNumberLengthDiffer);
        }
        return number;
    }


    public static Map<String, String> toMapFromQueryString(String queryString) {
        Map<String, String> map = new HashMap<>();

        if (queryString != null && !"".equals(queryString) && queryString.indexOf("&") > -1 && queryString.indexOf("=") > -1) {
            String[] arrTemp = queryString.split("&");
            for (String str : arrTemp) {
                String[] qs = str.split("=");
                map.put(qs[0], qs[1]);
            }
        }
        return map;
    }

    public static String getUuid() {
        UUID uuid = UUID.randomUUID();
        return uuid.toString().replaceAll("-", "");
    }
}

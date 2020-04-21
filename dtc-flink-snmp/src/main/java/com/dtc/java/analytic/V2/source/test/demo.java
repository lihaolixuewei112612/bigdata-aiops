package com.dtc.java.analytic.V2.source.test;

/**
 * Created on 2020-02-28
 *
 * @author :hao.li
 */
public class demo {
    public static void main(String[] args) {
        String str = "D:\\\\ Label:....  Serial Number d43be123";
        System.out.println(str.toCharArray());
        System.out.println(stringToAscii(str));

    }
    public static String stringToAscii(String value)
    {
        StringBuffer sbu = new StringBuffer();
        char[] chars = value.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if(i != chars.length - 1)
            {
                sbu.append((int)chars[i]).append(",");
            }
            else {
                sbu.append((int)chars[i]);
            }
        }
        return sbu.toString();
    }
}

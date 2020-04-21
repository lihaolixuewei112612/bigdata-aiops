package com.dtc.java.analytic.V1;

import com.alibaba.fastjson.JSON;

import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * Created on 2020-01-07
 *
 * @author :hao.li
 */
public class demo {
    public static void main(String[] args) throws ParseException {
        if(args[0].isEmpty()){
            System.out.println("lihao");
        }
       String startTime="20180910 22:58:10";
        System.out.println(startTime.length());
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
        System.out.println(String.valueOf(df.parse(startTime).getTime()).length());
    }
    public static boolean isJSON2(String str) {
        boolean result = false;
        try {
            Object obj= JSON.parse(str);
            result = true;
        } catch (Exception e) {
            result=false;
        }
        return result;
    }


}

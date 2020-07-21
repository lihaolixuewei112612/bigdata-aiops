package com.dtc.java.analytic.V2.worker;

/**
 * @Author : lihao
 * Created on : 2020-07-21
 * @Description : TODO描述类作用
 */
public class demo {
    public static void main(String[] args) {
        long now = System.currentTimeMillis() / 1000l;
        long daySecond = 60 * 60 * 24;
        long dayTime = now - (now + 8 * 3600) % daySecond;
        System.out.println(dayTime);
    }
}

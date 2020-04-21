package com.dtc.java.analytic.V2.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author : lihao
 * Created on : 2020-04-07
 * @Description : TODO描述类作用
 */
public class demo {
    public static void main(String[] args) {
        String[] str = {"abc","ab","c"};
  List<String> list = new ArrayList();
  list.add("ab");
  list.add("abc");
  list.add("abcd");
       Arrays.sort(str);
       for (int i=0;i<str.length;i++)
            System.out.println(str[i]);
    }
}

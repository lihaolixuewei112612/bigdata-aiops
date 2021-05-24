package com;

import com.apex_ids.util.MyEnum.MonthlyBillConf;

import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) {
    String str="26449.67,0.0,0.0,184.8,20200506,23.27;26542.07,0.0,0.0,92.4,20200507,23.27;26542.07,0.0,0.0,0.0,20200508,23.27";
        String[] dataArray = str.toString().replaceAll("\\s*", "").split(";");
        List<String> dataList=new ArrayList<>();
        for(String s:dataArray){//满足条件时间区间的才加入
                dataList.add(s);
        }
        List<String> list = new ArrayList<String>();
        list.add("a");

        // 使用构造器创建一个包含list的列表list1
        List<String> list1 = new ArrayList<String>(list);
        // 使用subList生成与list相同的列表list2
        List<String> list2 = list.subList(0, list.size());
        list2.add("b");

       list1.stream().forEach(e-> System.out.print(e));
        System.out.println();
        list2.stream().forEach(e-> System.out.print(e));
    }
}

package com.dtc.java.analytic.V2.source.mysql;

/**
 * @Author : lihao
 * Created on : 2020-04-21
 * @Description : TODO描述类作用
 */
class father{
    public String name = "父亲";
    public int age =30;
    public void show(){
        System.out.println("name ="+name+",age ="+age);
    }
}
class son extends father{
    public String name = "儿子";
    public int age =5;
    public void show(){
        System.out.println("name ="+name+",age ="+age);
    }
}
public class test1 {
    public static void main(String[] args) {
        father fa = new son();
        fa.show();
        System.out.println(fa.name);
    }
}

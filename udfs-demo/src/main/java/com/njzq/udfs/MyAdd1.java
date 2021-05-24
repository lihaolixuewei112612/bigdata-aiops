package com.njzq.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

@Description(name="myadd1",value="myadd(int a,int b) ==> return a+b",
        extended = "Example:\n"+
                " add(1,1) ==> 2 \n" +
                " add(1,2,3) ==> 6 ;")
public class MyAdd1 extends UDF {
    public int evaluate(int a,int b){
        return a+b;
    }
    public int evaluate(int a,int b,int c){
        return a+b+c;
    }
}

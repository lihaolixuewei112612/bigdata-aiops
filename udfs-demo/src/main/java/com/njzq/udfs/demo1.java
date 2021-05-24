package com.njzq.udfs;


import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class demo1 extends UDF{

    public boolean evaluate(){
        return true;
    }

    public boolean evaluate(int b){
        //int b=Integer.parseInt(a);
        if(b<0){
            return false;
        }
        if(b%2==0){
            return true;
        }else {
            return false;
        }

    }

    public boolean evaluate(String a){
        int b=Integer.parseInt(a);

        if(b<0){
            return false;
        }
        if(b%2==0){
            return true;
        }else {
            return false;
        }

    }

    public boolean evaluate(Text a){
        int b=Integer.parseInt(a.toString());

        if(b<0){
            return false;
        }
        if(b%2==0){
            return true;
        }else {
            return false;
        }

    }
    public boolean evaluate(Text t1,Text t2){
        //public boolean evaluate(String t1, String t2){
        if(t1==null || t2 ==null){
            return false;
        }

        double d1 = Double.parseDouble(t1.toString());
        double d2 = Double.parseDouble(t2.toString());
        /* double d1 = Double.parseDouble(t1);
         double d2 = Double.parseDouble(t2);*/
        if(d1>d2){
            return true;
        }else{
            return false;
        }
    }



    public boolean evaluate(String t1, String t2){
        if(t1==null || t2 ==null){
            return false;
        }

        double d1 = Double.parseDouble(t1);
        double d2 = Double.parseDouble(t2);
        if(d1>d2){
            return true;
        }else{
            return false;
        }
    }
}

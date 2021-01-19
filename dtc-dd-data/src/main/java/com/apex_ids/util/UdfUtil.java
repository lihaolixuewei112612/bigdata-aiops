package com.apex_ids.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UdfUtil {
    public static List getList(Object object){
        List list;
        if (object instanceof LazyArray){ //如果读取得是hive表数据，其数据为LazyArray
            list = ((LazyArray) object).getList();
        }else if(object instanceof LazyBinaryArray){
            list=((LazyBinaryArray) object).getList(); //LazyBinaryArray的情况时
        }else {
            list = (ArrayList) object;//与udaf结合使用时为ArrayList
        }
        return list;
    }
    public static Object parseObject(String object, String type){
        if (type.toLowerCase().equals("int")){
            return StringUtils.isBlank(object) ? 0 : Integer.valueOf(object).intValue();
        }else if (type.toLowerCase().equals("long")){
            return StringUtils.isBlank(object) ? 0l : Long.valueOf(object).longValue();
        }else if (type.toLowerCase().equals("float") || type.toLowerCase().equals("double")){
            return StringUtils.isBlank(object) ? 0d : Double.valueOf(object).doubleValue();
        }else if (type.toLowerCase().contains("decimal")){
            return StringUtils.isBlank(object) ? 0d : new BigDecimal(object).setScale(Integer.valueOf(getDecimalInfo(type)[1]), BigDecimal.ROUND_HALF_UP).doubleValue();
        }else if (type.toLowerCase().equals("string")){
            return parseString(object);
        }else {
            return parseString(object);
        }
    }
    public static String[] getDecimalInfo(String type){
        Matcher mat = Pattern.compile("(?<=\\()(\\S+)(?=\\))").matcher(type);
        while(mat.find()){
            return mat.group().split("\\,");
        }
        return null;
    }
    public static PrimitiveObjectInspector getInspector(String type){
        if (type.toLowerCase().equals("int")){
            return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        }else if (type.toLowerCase().equals("long")){
            return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
        }else if (type.toLowerCase().equals("float") || type.toLowerCase().equals("double")){
            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        }else if (type.toLowerCase().contains("decimal")){
            return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        }else if (type.toLowerCase().equals("string")){
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }else {
            return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
        }
    }
    public static String parseString(String object){
        try{
            if (StringUtils.isBlank(object)){
                return null;
            }else if (object.length() == 0){
                return null;
            }else if (object.toLowerCase().equals("null")){
                return null;
            }else {
                return object;
            }
        }catch (NullPointerException e){
            return null;
        }

    }
}

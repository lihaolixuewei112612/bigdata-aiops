package com.njzq.udfs;


import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class MyNvl extends GenericUDF {

    private GenericUDFUtils.ReturnObjectInspectorResolver returnOIResolver;
    private ObjectInspector[] argumentOIs;

    @Override   //  初始化方法，被调用一次
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //  赋值
        this.argumentOIs=arguments;
        //  校验参数个数
        if(arguments.length!=2){
            throw new UDFArgumentLengthException("we need 2 args!");
        }
        //  类型存储器 ( 用 update 方法来更新自己的类型 )
        returnOIResolver = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
        //  校验参数类型( 两个参数的类型是否一致 )
        if(!(returnOIResolver.update(arguments[0]) && returnOIResolver.update(arguments[1]))){
            throw new UDFArgumentTypeException(2,"2 args should be same type!");
        }
        return returnOIResolver.get();
    }

    @Override   //  主要逻辑代码块
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //  参数 1 ：
        Object retVal = returnOIResolver.convertIfNecessary(arguments[0].get(),argumentOIs[0]);
        //  处理
        if(retVal==null){
            retVal = returnOIResolver.convertIfNecessary(arguments[1].get(),argumentOIs[1]);
        }
        return retVal;
    }

    @Override   //  展示调试信息
    public String getDisplayString(String[] children) {
        StringBuffer sb = new StringBuffer();
        sb.append("arguments length :"+argumentOIs.length);
        return sb.toString();
    }
}

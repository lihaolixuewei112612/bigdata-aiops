package com.njzq.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class MyAdd2 extends GenericUDF {
    //  输入类型 int
    private transient IntObjectInspector arg0;
    //  输入类型 int
    private transient IntObjectInspector arg1;
    //  返回值类型
    private IntWritable res;
    @Override   //  初始化方法，定义返回值类型
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        //  输入类型
        this.arg0 = (IntObjectInspector)arguments[0];
        this.arg1 = (IntObjectInspector)arguments[1];
        //  返回值类型：int
        this.res = new IntWritable(0);
        //  确定返回值类型：int
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    }

    @Override   //  逻辑处理
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        //  取值
        int r = 0;
        for (DeferredObject argument : arguments) {
            Object a = argument.get();
            int i = this.arg0.get(a);
            r+=i;
        }
        res.set(r);
        return res;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "nothing";
    }
}

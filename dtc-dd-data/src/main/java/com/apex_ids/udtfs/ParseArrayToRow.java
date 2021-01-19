package com.apex_ids.udtfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.lazy.LazyArray;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

public class ParseArrayToRow extends GenericUDTF {
    public ParseArrayToRow() {
        super();
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        ArrayList<String> fields = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        fields.add("rq");
        outOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fields.add("zxj");
        outOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fields.add("zsp");
        outOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fields.add("zgj");
        outOIS.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fields, outOIS);
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List list = getList(args[1]);
        Object[] result = new Object[5];
        result[0] = args[0];
        for (int j=0; j<list.size(); j++){
            List<Object> tmp = getList(list.get(j));
            for (int i=0; i<tmp.size(); i++){
                result[i+1] = tmp.get(i);
            }
            forward(result);
        }
    }
    private List getList(Object object){
        List list;
        if (object instanceof LazyArray){ //如果读取得是hive表数据，其数据为LazyArray
            list = ((LazyArray) object).getList();
        }else {
            list = (ArrayList) object;
        }
        return list;
    }
    @Override
    public void close() throws HiveException {

    }
}

package com.njzq.udfs;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryArray;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;

import java.util.*;

public class demo extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length < 1){
            throw new UDFArgumentTypeException(info.length-1, "No argument.");
        }
        return new CollectListUDAFEvaluator();
    }
    public static class CollectListUDAFEvaluator extends GenericUDAFEvaluator {
        protected PrimitiveObjectInspector inputKeyOI;
        protected StandardListObjectInspector loi;
        protected StandardListObjectInspector internalMergeOI;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters)
                throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1) {
                inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
            } else {
                if ( parameters[0] instanceof StandardListObjectInspector ) {
                    internalMergeOI = (StandardListObjectInspector) parameters[0];
                    inputKeyOI = (PrimitiveObjectInspector) internalMergeOI.getListElementObjectInspector();
                    loi = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(internalMergeOI);
                    return loi;
                } else {
                    inputKeyOI = (PrimitiveObjectInspector) parameters[0];
                    return ObjectInspectorFactory.getStandardListObjectInspector(
                            ObjectInspectorUtils.getStandardObjectInspector(inputKeyOI));
                }
            }
        }

        static class MkListAggregationBuffer implements AggregationBuffer {
            List<Object> container = Lists.newArrayList();
        }
        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((MkListAggregationBuffer) agg).container.clear();
        }
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            MkListAggregationBuffer ret = new MkListAggregationBuffer();
            return ret;
        }
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters)
                throws HiveException {
            if(parameters == null || parameters.length != 1){
                return;
            }
            Object key = parameters[0];
            if (key != null) {
                MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
                putIntoList(key, myagg.container);
            }
        }

        private void putIntoList(Object key, List<Object> container) {
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(key,  this.inputKeyOI);
            container.add(pCopy);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg)
                throws HiveException {
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            List<Object> ret = Lists.newArrayList(myagg.container);
            return ret;
        }
        @Override
        public void merge(AggregationBuffer agg, Object partial)
                throws HiveException {
            if(partial == null){
                return;
            }
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            List<Object> partialResult = (List<Object>) internalMergeOI.getList(partial);
            for (Object ob: partialResult) {
                putIntoList(ob, myagg.container);
            }
            return;
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MkListAggregationBuffer myagg = (MkListAggregationBuffer) agg;
            Map<Text, Integer> map = Maps.newHashMap();
            for (int i = 0; i< myagg.container.size() ; i++){
                Text key = (Text) myagg.container.get(i);
                if (map.containsKey(key)) {
                    map.put(key, map.get(key) + 1);
                }else{
                    map.put(key, 1);
                }
            }
            List<Map.Entry<Text, Integer>> listData = Lists.newArrayList(map.entrySet());
            Collections.sort(listData, new Comparator<Map.Entry<Text, Integer>>() {
                public int compare(Map.Entry<Text, Integer> o1, Map.Entry<Text, Integer> o2) {
                    if (o1.getValue() < o2.getValue())
                        return 1;
                    else if (o1.getValue() == o2.getValue())
                        return 0;
                    else
                        return -1;
                }
            });

            List<Object> ret =  Lists.newArrayList();
            for(Map.Entry<Text, Integer> entry : listData){
                ret.add(entry.getKey());
                ret.add(new Text(entry.getValue().toString()));
            }
            return ret;
        }
    }
}

package com.apex_ids.udafs;

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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GroupRow extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length < 1){
            throw new UDFArgumentTypeException(info.length-1, "No argument.");
        }
        return new GenericUDAFGroupRowEvaluator();
    }
    public static class GenericUDAFGroupRowEvaluator extends GenericUDAFEvaluator{
        private StandardListObjectInspector intenalMergeOI;
        static class ArrayAggregationBuffer implements AggregationBuffer{
            List<List<Object>> container;
        }
        public void putIntoList(List<Object> param, ArrayAggregationBuffer myAgg){
            List<Object> list = new ArrayList<>();
            for (int i=0; i<param.size(); i++){
                list.add(ObjectInspectorUtils.copyToStandardObject(param.get(i), PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)));
            }
            myAgg.container.add(list);
        }


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)
                        )
                );
            }else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
                intenalMergeOI = (StandardListObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        ObjectInspectorFactory.getStandardListObjectInspector(
                                PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)
                        )
                );
            }else {
                throw new RuntimeException("no such mode Exception");
            }
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            putIntoList(Arrays.asList(parameters), (ArrayAggregationBuffer) agg);
//            System.out.println("---------iterate----------");
//            System.out.println(Arrays.asList(parameters));
//            System.out.println(((ArrayAggregationBuffer) agg).container);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            return myAgg.container;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;

            List<Object> partialResult = (List<Object>) this.intenalMergeOI.getList(partial);
            for (Object obj: partialResult){
                if (obj instanceof LazyBinaryArray){
                    putIntoList(((LazyBinaryArray) obj).getList(), myAgg);
                }else{
                    putIntoList((ArrayList)obj, myAgg);
                }

            }

        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            return myAgg.container;
        }
    }
}

package com.apex_ids.udafs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFAverage;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import java.util.ArrayList;
import java.util.List;

@Description(
        name = "UdafTest",
        value = "",
        extended = "> select _FUNC_(col) from src;"
)
public class UdafTest extends AbstractGenericUDAFResolver {
    public UdafTest(){}

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        if (info.length < 1){
            throw new UDFArgumentTypeException(info.length-1, "No argument.");
        }
        if (info[0].getCategory() != ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but " +
                            info[0].getTypeName() + " was passed as parameter 1.");
        }
        return new GenericUDAFCollectEvaluator();
    }
    @SuppressWarnings("deprecation")
    public static class GenericUDAFCollectEvaluator extends GenericUDAFEvaluator{
        private PrimitiveObjectInspector inputOI;
        private StandardListObjectInspector intenalMergeOI;
        private StandardListObjectInspector loi;
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE){
                inputOI = (PrimitiveObjectInspector) parameters[0];
                return ObjectInspectorFactory.getStandardListObjectInspector(
                        (PrimitiveObjectInspector)ObjectInspectorUtils.getStandardObjectInspector(inputOI)
                );
            }else if (m == Mode.PARTIAL2 || m == Mode.FINAL){
                intenalMergeOI = (StandardListObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) intenalMergeOI.getListElementObjectInspector();
                loi = ObjectInspectorFactory.getStandardListObjectInspector(inputOI);
                return loi;
            }
            return null;
        }
        static class ArrayAggregationBuffer implements AggregationBuffer{
            List<Object> container;
        }
        public void putIntoList(Object param, ArrayAggregationBuffer myAgg){
            Object pCopy = ObjectInspectorUtils.copyToStandardObject(param, this.inputOI);
            myAgg.container.add(pCopy);
        }
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            ArrayAggregationBuffer ret = new ArrayAggregationBuffer();
            reset(ret);
            return ret;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            ((ArrayAggregationBuffer) agg).container = new ArrayList<Object>();
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            Object p = parameters[0];
            if (p != null){
                putIntoList(p, (ArrayAggregationBuffer) agg);
            }
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> list = new ArrayList<Object>();
            list.addAll(myAgg.container);
            return list;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> partialResult = (ArrayList<Object>) this.intenalMergeOI.getList(partial);
            for (Object obj: partialResult){
                putIntoList(obj, myAgg);
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            ArrayAggregationBuffer myAgg = (ArrayAggregationBuffer) agg;
            ArrayList<Object> list = new ArrayList<Object>();
            list.addAll(myAgg.container);
            return list;
        }
    }
}

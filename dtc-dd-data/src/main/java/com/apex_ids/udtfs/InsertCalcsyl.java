package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.CustCalsylIn;
import com.apex_ids.util.MyEnum.CustCalsylOut;
import com.apex_ids.util.UdfUtil;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.math.BigDecimal;
import java.util.*;

/**使用方法
 * 获得区间收益率
 * create function udfs.InsertCalcsyl as 'com.apex.ids.udtfs.InsertCalcsyl' USING JAR 'hdfs://master:8020/user/hive/udfs/udfs-all-1.0.0.jar';
 * select udfs.GetSyl(row,20190501,20190531,20180102) from(select khh,udfs.GroupRow(khh,rq,zzc,zzc_jzjy,zzc_rzrq,zzc_ggqq,zjye,zjye_jzjy,zjye_rzrq,zjye_ggqq,zcjlr,crje,qcje,zrzqsz,zczqsz,yk,yk_jzjy,yk_rzrq,yk_jrcp,yk_ggqq,zqsz,zqsz_jzjy,zqsz_rzrq,zqsz_jrcp,zqsz_ggqq,zfz,zfz_rzrq,zxjz,zxjz_jzjy,zxjz_rzrq,zxjz_ggqq) as row from cust.t_stat_zd_r where rq between 20190501 and 20190531 group by khh)a;
 */
public class InsertCalcsyl extends GenericUDTF {
    private static int syrq;
    private static int dayNum;
    private static int yearNum;
    private static Integer yf;
    private static int initDate=20180102;


    @Override
    public StructObjectInspector initialize(ObjectInspector[] inOIs) throws UDFArgumentException {
        if (inOIs.length < 1){
            throw new UDFArgumentException("没有数据咯");
        }
        return putFormat();
    }

    @Override
    public void process(Object[] row) throws HiveException {

        List<List<Object>> bills = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据
        for (int i=0; i<rows.size(); i++){
            List<String> bill = (List<String>) rows.get(i);//转换
            bills.add(tranDataType(bill));
        }
        sort(bills);//排序
        List<Object> result = cost(bills);//计算
        forward(result.toArray());
        /*Object[] objects = new Object[CustCalsylOut.values().length];
        Arrays.fill(objects, 0D);
        forward(objects);*/
    }

    @Override
    public void close() throws HiveException {

    }

    private List<Object> cost(List<List<Object>> bills){
        List<Object> result = getList();
        StringBuilder sb=new StringBuilder();
        String khh="";
        for(List<Object> l:bills){
            khh=l.get(0)+"";
            sb.append(new BigDecimal(l.get(1)+"")+","+new BigDecimal(l.get(2)+"")+","+
                    new BigDecimal(l.get(3)+"")+","+new BigDecimal(l.get(4)+"")+","+l.get(5)+","+l.get(6)+";");
            /*sb.append(new BigDecimal(l.get(1)+"")+","+new BigDecimal(l.get(2)+"")+","+
                    new BigDecimal(l.get(3)+"")+","+new BigDecimal(l.get(4)+"")+","+l.get(5)+","+l.get(6)+","+l.get(7)+","+l.get(8)+","+l.get(9)+
                    ","+l.get(10)+","+l.get(11)+";");*/
        }
        result.set(0,khh);
        result.set(1,sb.toString());
        return result;
    }
    private static double[] tranListToArray(List<Double> data){
        double[] tmp = new double[data.size()];
        for (int i=0; i<data.size(); i++){
            tmp[i] = data.get(i);
        }
        return tmp;
    }
    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (CustCalsylOut conf: CustCalsylOut.values()){
            fieldNames.add(conf.toString());
            outOIS.add(UdfUtil.getInspector(conf.getType()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, outOIS);
    }
    private List<Object> tranDataType(List<String> bill){
        /**
         * 字段格式标准化
         */
        List<Object> result = new ArrayList<>();
        for (int i=0; i<bill.size(); i++){
            result.add(UdfUtil.parseObject(bill.get(i), CustCalsylIn.values()[i].getType()));
        }
        return result;
    }
    private void sort(List<List<Object>> bills){
        /**
         * 排序
         */
        Collections.sort(bills,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        Integer d1Str = Integer.valueOf(d1.get(CustCalsylIn.rq.getIndex()).toString());
                        Integer d2Str = Integer.valueOf(d2.get(CustCalsylIn.rq.getIndex()).toString());
                        return d1Str.compareTo(d2Str);
                    }
                });
    }
    public List<Object> getList(){
        Object[] objects = new Object[CustCalsylOut.values().length];
        Arrays.fill(objects, 0D);
        return Arrays.asList(objects);
    }

}

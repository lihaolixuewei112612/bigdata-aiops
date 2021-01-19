package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.AnalyCwkzIn;
import com.apex_ids.util.MyEnum.YearBillConfHengtaiOut;
import com.apex_ids.util.UdfUtil;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by caigx
 * Date: 2019/11/22
 * Time: 10:13
 * Description: No Description
 */
public class Analy_cwkz extends GenericUDTF {
    private static List<String> result=null;
    private static final Logger log = LoggerFactory.getLogger(Analy_cwkz.class);
    /*public static void initCache( ){
        Properties props = new Properties();
        Integer year=0;
        try {
            props.load(YearBill_Hengtai.class.getResourceAsStream("/properties/yearbill.properties"));
            year=Integer.parseInt(props.getProperty("yearBill_year"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        result=getYearHS300(Integer.parseInt(year+"0101"),Integer.parseInt(year+"1231"));

    }
    static {
        initCache();
    }*/
    @Override
    public void configure(MapredContext mapredContext) {
        super.configure(mapredContext);
    }

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
        List<Object> monthlyBill = caculate(bills);//计算
        forward(monthlyBill.toArray());
    }

    @Override
    public void close() throws HiveException {

    }
    private List<Object> caculate(List<List<Object>> bills) {
        String khh=bills.get(0).get(AnalyCwkzIn.khh.getIndex())+"";
        int ksrq = (int)bills.get(0).get(AnalyCwkzIn.rq.getIndex());
        List khData=new ArrayList();
        List hs300Data=new ArrayList();
        Integer year=ksrq/10000;
        /*if(result==null||result.isEmpty()){
            initCache(year);
        }*/
        int jsrq = (int)bills.get(bills.size()-1).get(AnalyCwkzIn.rq.getIndex());
        for (List<Object> bill: bills){
                    khData.add(Double.parseDouble(bill.get(AnalyCwkzIn.zqsz_jzjy.getIndex())+"")
                            +Double.parseDouble(bill.get(AnalyCwkzIn.zqsz_rzrq.getIndex())+""));
                    hs300Data.add(Double.parseDouble(bill.get(AnalyCwkzIn.zshq_hs300.getIndex())+""));
        }
        /*for (List<Object> bill: bills){
            for(String m:result){
                if(bill.get(AnalyCwkzIn.rq.getIndex()).toString().equals(m.split(":")[0])){
                    khData.add(Double.parseDouble(bill.get(AnalyCwkzIn.zqsz_jzjy.getIndex())+"")
                            +Double.parseDouble(bill.get(AnalyCwkzIn.zqsz_rzrq.getIndex())+""));
                    hs300Data.add(Double.parseDouble(m.split(":")[1]));
                    break;
                }
            }
        }*/
        boolean flag=false;
        /*for(Object i:khData){
            if(Double.parseDouble(i+"") != 0.0d){
                flag=true;
                break;
            }
        }
        double score =0.0d;
        if(flag){*/
        double   score =getPearsonCorrelationScore(khData, hs300Data);
        /*}*/
        List<Object> result=getList();
        result.set(YearBillConfHengtaiOut.khh.getIndex(),khh);
        result.set(YearBillConfHengtaiOut.order_id.getIndex(),17.0d);
        result.set(YearBillConfHengtaiOut.zbz.getIndex(),score);
        result.set(YearBillConfHengtaiOut.zbmc.getIndex(),"cwkz");
        return result;
    }
    public List<Object> getList(){
        Object[] objects = new Object[YearBillConfHengtaiOut.values().length];
        Arrays.fill(objects, 0D);
        return Arrays.asList(objects);
    }

    private void sort(List<List<Object>> bills){
        /**
         * 排序
         */
        Collections.sort(bills,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        Integer d1Str = Integer.valueOf(d1.get(AnalyCwkzIn.rq.getIndex()).toString());
                        Integer d2Str = Integer.valueOf(d2.get(AnalyCwkzIn.rq.getIndex()).toString());
                        return d1Str.compareTo(d2Str);
                    }
                });
    }
    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (YearBillConfHengtaiOut conf: YearBillConfHengtaiOut.values()){
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
            result.add(UdfUtil.parseObject(bill.get(i), AnalyCwkzIn.values()[i].getType()));
        }
        return result;
    }
    private static double[] tranListToArray(List<Double> data){
        double[] tmp = new double[data.size()];
        for (int i=0; i<data.size(); i++){
            tmp[i] = data.get(i);
        }
        return tmp;
    }


    /*private static List<String>getYearHS300(int ksrq,int jsrq){
        String sqlBuf="select rq,zxj from info.this_zshq  where rq between "+ksrq+"  and "+jsrq+" and zsdm='399300' order by rq";
        Properties props = new Properties();
        Connection conn = null;
        List<String> result=new ArrayList<>();
        try {
            props.load(Analy_cwkz.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url").replace("dsc_cfg", "info"), props);
            PreparedStatement ps = conn.prepareStatement(sqlBuf);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                result.add(rs.getInt(1)+":"+rs.getString(2));
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.err.println(e);
                }
            }
        }
        return  result;
    }*/

    public Analy_cwkz() {
        super();
    }

    public static double getPearsonCorrelationScore(List x, List y) {
        if (x.size() != y.size())
            throw new RuntimeException("数据不正确！");
        double[] xData = new double[x.size()];
        double[] yData = new double[x.size()];
        for (int i = 0; i < x.size(); i++) {
            xData[i] = (double)x.get(i);
            yData[i] = (double)y.get(i);
        }
        return getPearsonCorrelationScore(xData,yData);
    }
    public static double getPearsonCorrelationScore(double[] xData, double[] yData) {
        if (xData.length != yData.length)
            throw new RuntimeException("数据不正确！");
        double xMeans;
        double yMeans;
        double numerator = 0;// 求解皮尔逊的分子
        double denominator = 0;// 求解皮尔逊系数的分母

        double result = 0;
        // 拿到两个数据的平均值
        xMeans = getMeans(xData);
        yMeans = getMeans(yData);
        // 计算皮尔逊系数的分子
        numerator = generateNumerator(xData, xMeans, yData, yMeans);
        // 计算皮尔逊系数的分母
        denominator = generateDenomiator(xData, xMeans, yData, yMeans);
        // 计算皮尔逊系数
        result = numerator / denominator;
        return result;
    }
    /**
     * 计算分子
     *
     * @param xData
     * @param xMeans
     * @param yData
     * @param yMeans
     * @return
     */
    private static double generateNumerator(double[] xData, double xMeans, double[] yData, double yMeans) {
        double numerator = 0.0;
        for (int i = 0; i < xData.length; i++) {
            numerator += (xData[i] - xMeans) * (yData[i] - yMeans);
        }
        return numerator;
    }

    /**
     * 生成分母
     *
     * @param yMeans
     * @param yData
     * @param xMeans
     * @param xData
     * @return 分母
     */
    private static double generateDenomiator(double[] xData, double xMeans, double[] yData, double yMeans) {
        double xSum = 0.0;
        for (int i = 0; i < xData.length; i++) {
            xSum += (xData[i] - xMeans) * (xData[i] - xMeans);
        }
        double ySum = 0.0;
        for (int i = 0; i < yData.length; i++) {
            ySum += (yData[i] - yMeans) * (yData[i] - yMeans);
        }
        return Math.sqrt(xSum) * Math.sqrt(ySum);
    }

    /**
     * 根据给定的数据集进行平均值计算
     *
     * @param
     * @return 给定数据集的平均值
     */
    private static double getMeans(double[] datas) {
        double sum = 0.0;
        for (int i = 0; i < datas.length; i++) {
            sum += datas[i];
        }
        return sum / datas.length;
    }
}

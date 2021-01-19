package com.apex_ids.udtfs.hengtai;

import com.alibaba.fastjson.JSONObject;
import com.apex_ids.util.MyEnum.YearBillConfHengtaiIn;
import com.apex_ids.util.MyEnum.YearBillConfHengtaiOut;
import com.apex_ids.util.UdfUtil;
import com.apexsoft.quotaMethod.QuotaFunction;
import com.apexsoft.utils.DateUtils;
import com.google.common.primitives.Ints;
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

import java.sql.*;
import java.util.*;

/**
 * Created by caigx
 * Date: 2019/11/22
 * Time: 10:13
 * Description: No Description
 */
public class YearBill_Hengtai extends GenericUDTF {
    private static final Logger logger= LoggerFactory.getLogger(YearBill_Hengtai.class);
    private static TradingDays tradingDays;
    private static double syl_300=0.0d;
    private static List<Double> syl_my_300=new ArrayList<>();
    private static String zsdm="399300";
    private static double noRiskRate =0.033;

    static {
        initCache();
    }
    public static void initCache(){
            Properties props = new Properties();
            Integer year=0;
            try {
                props.load(YearBill_Hengtai.class.getResourceAsStream("/properties/yearbill.properties"));
                year=Integer.parseInt(props.getProperty("yearBill_year"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            new YearBill_Hengtai().initTradingDays();//初始化交易日
            int ncrq = Integer.parseInt(year + "0101");
            int nmrq = Integer.parseInt(year + "1231");
            syl_300 = getMarketSyl(zsdm, ncrq, nmrq);
            for (int i = 1; i <= 9; i++) {
                int ycrq = Integer.parseInt(year + "0" + i + "01");
                int ymrq = Integer.parseInt(year + "0" + i + "31");
                syl_my_300.add(getMarketSyl(zsdm, ycrq, ymrq));
            }
            for (int i = 10; i <= 12; i++) {
                int ycrq = Integer.parseInt(year + "" + i + "01");
                int ymrq = Integer.parseInt(year + "" + i + "31");
                try{
                    syl_my_300.add(getMarketSyl(zsdm, ycrq, ymrq));
                }catch (Exception e){
                    syl_my_300.add(0.0d);
                }
            }
    }
    private void initTradingDays(){
        tradingDays = new TradingDays();
    }
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
        if(tradingDays==null){
            tradingDays=new TradingDays();
        }
        List<List<Object>> bills = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据
        for (int i=0; i<rows.size(); i++){
            List<String> bill = (List<String>) rows.get(i);//转换
            bills.add(tranDataType(bill));
        }
        sort(bills);//排序
        List<List<Object>> monthlyBill = caculate(bills);//计算
        for(List<Object> result:monthlyBill){
            forward(result.toArray());
        }
    }

    @Override
    public void close() throws HiveException {

    }
    private List<List<Object>> caculate(List<List<Object>> bills) {
        List<List<Object>> list=new ArrayList<>();
        List<Object> result = getList();
        List<Object> result_hs300 = getList();
        List<Object> result_bj_hs300 = getList(); //沪深300的比较
        List<Object> result_syl_my = getList(); //每月收益率
        List<Object> result_synl = getList();
        List<Object> result_fxkz = getList();//风险控制
        List<Object> result_my_hs300 = getList();//沪深300每月
        List<Object> result_my_sy = getList();//每月收益
        double sylMax=3.0d;
        double sylMin=-2.0d;
        String khh=bills.get(0).get(YearBillConfHengtaiIn.khh.getIndex())+"";
        int ksrq = (int)bills.get(0).get(YearBillConfHengtaiIn.rq.getIndex());
        String year=ksrq/10000+"";
        int jsrq = (int)bills.get(bills.size()-1).get(YearBillConfHengtaiIn.rq.getIndex());

        int month_start=Integer.parseInt((ksrq+"").substring(4,6)); //开始月份
        int month_end=Integer.parseInt((jsrq+"").substring(4,6));
        Map<Integer,Double> map_temp_my_syl=new HashMap();
        int ncrq = Integer.parseInt(year + "0101");
        int nmrq = Integer.parseInt(year + "1231");
        Map map_my_syl_hs300=new LinkedHashMap();

        /**
         * 收益率——个人
         */
        List<Map<String, Object>> fundList = new ArrayList<>();
        for (List<Object> bill: bills){
            fundList.add(getCustFund(bill));
        }
        //带入公式计算盈亏率
        double syl_gr=0.0d;
        if(fundList.size()!=0){
            syl_gr=QuotaFunction.getSyl(fundList);
        }
        /*syl_gr=syl_gr<sylMin?sylMin:syl_gr;
        syl_gr=syl_gr>sylMax?sylMax:syl_gr;*/
        if(syl_gr<sylMin){
            syl_gr=sylMin;
        }else if( syl_gr> sylMax){
            syl_gr=sylMax;
        }
        String syl = String.format("%.6f", syl_gr);
        result.set(YearBillConfHengtaiOut.zbmc.getIndex(),"syl");
        result.set(YearBillConfHengtaiOut.zbz.getIndex(),syl);
        result.set(YearBillConfHengtaiOut.order_id.getIndex(),1.0d);
        list.add(result);

        /*if(syl_300==0.0d){
            syl_300=getMarketSyl(zsdm,ncrq,nmrq);
        }*/
        result_hs300.set(YearBillConfHengtaiOut.zbmc.getIndex(),"syl_hs300");
        result_hs300.set(YearBillConfHengtaiOut.zbz.getIndex(),String.format("%.6f",syl_300));
        result_hs300.set(YearBillConfHengtaiOut.order_id.getIndex(),2.0d);
        list.add(result_hs300);
        /**
         * 收益率与300收益率比较
         */
        result_bj_hs300.set(YearBillConfHengtaiOut.zbmc.getIndex(),"syl_bj_hs300");
        result_bj_hs300.set(YearBillConfHengtaiOut.zbz.getIndex(),String.format("%.6f",syl_gr-syl_300));
        result_bj_hs300.set(YearBillConfHengtaiOut.order_id.getIndex(),3.0d);
        list.add(result_bj_hs300);

        /**
         * 每月收益率
         */
        Map map_month=new LinkedHashMap();
        Map map_sy_month=new LinkedHashMap();//每月收益
        List<Double> listsharp=new ArrayList();
        boolean flag=false;
        double sum_sy_my=0.0d;
        System.out.println("month_start:"+month_start+"month_end:"+month_end);
        for(int i=month_start;i<month_end;i++){//统计每月收益率
            List<Map<String, Object>> fundList_month = new ArrayList<>();
            List<Double> sy_my=new ArrayList();//装每月收益
            sum_sy_my=0.0d;
            for (List<Object> bill: bills){
                if(i==Integer.parseInt((bill.get(YearBillConfHengtaiIn.rq.getIndex())+"").substring(4,6))){
                    fundList_month.add(getCustFund(bill));
                    sy_my.add(Double.parseDouble(bill.get(YearBillConfHengtaiIn.yk.getIndex())+""));
                }
                if(i<Integer.parseInt((bill.get(YearBillConfHengtaiIn.rq.getIndex())+"").substring(4,6))){
                    break;
                }

            }
            double syl_month=0.0d;
            if(fundList_month.size()!=0){
                syl_month=QuotaFunction.getSyl(fundList_month);
            }
            /*syl_month=syl_month<sylMin?sylMin:syl_month;
            syl_month=syl_month>sylMax?sylMax:syl_month;*/
            if(syl_month>sylMax){
                syl_month=sylMax;
            }else if(syl_month<sylMin){
                syl_month=sylMin;
            }

            String _month=i<10?"0"+(i):(i)+"";
            map_month.put(year+_month,String.format("%.6f",syl_month));
            listsharp.add(Double.parseDouble(String.format("%.4f",syl_month)));//sharp
            map_temp_my_syl.put(i,syl_month);

            for(Double d:sy_my){
                sum_sy_my+=d;
            }
            String __month=i<10?"0"+(i):(i)+"";
            map_sy_month.put(year+__month,String.format("%.2f",sum_sy_my));
        }
        List<Map<String, Object>> fundList_month = new ArrayList<>();
        List<Double> sy_my = new ArrayList<>();
        for (List<Object> bill: bills) {    //最末月的收益率
            if (month_end == Integer.parseInt((bill.get(YearBillConfHengtaiIn.rq.getIndex()) + "").substring(4,6))) {
                fundList_month.add(getCustFund(bill));
                sy_my.add(Double.parseDouble(bill.get(YearBillConfHengtaiIn.yk.getIndex())+""));
            }
        }
        double syl_month=QuotaFunction.getSyl(fundList_month);
        sum_sy_my=0.0d;
        /*syl_month=syl_month<sylMin?sylMin:syl_month;
        syl_month=syl_month>sylMax?sylMax:syl_month;*/
        if(syl_month>sylMax){
            syl_month=sylMax;
        }else if(syl_month<sylMin){
            syl_month=sylMin;
        }
        map_month.put(year+month_end,String.format("%.6f", syl_month));
        for(Double d:sy_my){
            sum_sy_my+=d;
        }
        map_sy_month.put(year+month_end,String.format("%.2f",sum_sy_my));
        map_temp_my_syl.put(month_end,syl_month);
        listsharp.add(Double.parseDouble(String.format("%.4f",syl_month)));//sharp
        result_syl_my.set(YearBillConfHengtaiOut.zbmc.getIndex(),"syl_my");
        result_syl_my.set(YearBillConfHengtaiOut.zbz.getIndex(),new JSONObject(map_month));
        result_syl_my.set(YearBillConfHengtaiOut.order_id.getIndex(),7.0d);
        list.add(result_syl_my);

        /**
         * 每月收益
         */
        result_my_sy.set(YearBillConfHengtaiOut.zbmc.getIndex(),"sy_my");
        result_my_sy.set(YearBillConfHengtaiOut.zbz.getIndex(),new JSONObject(map_sy_month));
        result_my_sy.set(YearBillConfHengtaiOut.order_id.getIndex(),20.0d);
        list.add(result_my_sy);
        /**
         * 收益能力
         */
        /*if(syl_my_300.size()==0){

        }*/
        double syl_sum=0.0d;
        for(int i=0;i<syl_my_300.size();i++){
            if(map_temp_my_syl.get(i+1)!=null){
                syl_sum+=map_temp_my_syl.get(i+1)-syl_my_300.get(i);
            }
        }
        result_synl.set(YearBillConfHengtaiOut.zbmc.getIndex(),"synl");
        result_synl.set(YearBillConfHengtaiOut.zbz.getIndex(),String.format("%.6f",syl_sum));
        result_synl.set(YearBillConfHengtaiOut.order_id.getIndex(),15.0d);
        list.add(result_synl);
        /**
         * 沪深300每月收益率
         */
        for(int i=0;i<syl_my_300.size();i++){
            String _month=i+1<10?"0"+(i+1):(i+1)+"";
            map_my_syl_hs300.put(year+_month,String.format("%.6f",syl_my_300.get(i)));
        }
        result_my_hs300.set(YearBillConfHengtaiOut.zbmc.getIndex(),"syl_my_hs300");
        result_my_hs300.set(YearBillConfHengtaiOut.zbz.getIndex(),new JSONObject(map_my_syl_hs300));
        result_my_hs300.set(YearBillConfHengtaiOut.order_id.getIndex(),23.0d);
        list.add(result_my_hs300);
        /**
         * 风险控制
         */
        //double[] sylDouble = getSylDoubles(fundList, tradingDays);
        double[] sylDouble=QuotaFunction.tranListToArray(listsharp);
        double sharp=sylDouble!=null&&sylDouble.length!=0?QuotaFunction.calcSharpRatio(sylDouble, noRiskRate):0.0d;
        result_fxkz.set(YearBillConfHengtaiOut.zbmc.getIndex(),"fxkz");
        result_fxkz.set(YearBillConfHengtaiOut.zbz.getIndex(),String.format("%.6f",sharp));
        result_fxkz.set(YearBillConfHengtaiOut.order_id.getIndex(),19.0d);
        list.add(result_fxkz);

        for(List<Object> l:list){
            l.set(YearBillConfHengtaiOut.khh.getIndex(),khh);
            //l.set(YearBillConfHengtaiOut.year.getIndex(),ksrq/10000);
        }
        return list;
    }
    public List<Object> getList(){
        Object[] objects = new Object[YearBillConfHengtaiOut.values().length];
        Arrays.fill(objects, 0D);
        return Arrays.asList(objects);
    }
    private static Map<String, Object> getCustFund(List<Object> bill){
        Map<String, Object> map = new HashMap<>();
        map.put("RQ", bill.get(YearBillConfHengtaiIn.rq.getIndex()));
        map.put("ZZC", bill.get(YearBillConfHengtaiIn.zzc.getIndex()));
        map.put("ZCJLR", bill.get(YearBillConfHengtaiIn.zcjlr.getIndex()));
        map.put("ZXJZ", bill.get(YearBillConfHengtaiIn.zxjz.getIndex()));
        map.put("ZFZ",bill.get(YearBillConfHengtaiIn.zfz.getIndex()));
        map.put("DRYK", bill.get(YearBillConfHengtaiIn.yk.getIndex()));
        map.put("ZJYE",bill.get(YearBillConfHengtaiIn.zjye.getIndex()));
        return map;
    }
    private void sort(List<List<Object>> bills){
        /**
         * 排序
         */
        Collections.sort(bills,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        Integer d1Str = Integer.valueOf(d1.get(YearBillConfHengtaiIn.rq.getIndex()).toString());
                        Integer d2Str = Integer.valueOf(d2.get(YearBillConfHengtaiIn.rq.getIndex()).toString());
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
            result.add(UdfUtil.parseObject(bill.get(i), YearBillConfHengtaiIn.values()[i].getType()));
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
    private static double[] getSylDoubles(List<Map<String, Object>> data, TradingDays tradingDays){
        int end = Integer.valueOf(data.get(data.size()-1).get("RQ").toString());
        int ksrq = Integer.valueOf(data.get(0).get("RQ").toString());
        ArrayList sylList = new ArrayList();
        int jsrq;
        do {
            jsrq = tradingDays.findTradingDay(DateUtils.getNextMonth(ksrq, -1), 0);
            List<Integer> index = getIndexFromCustBills(data, ksrq, jsrq);
            if (index.size() == 2){
                List<Map<String, Object>> sub = data.subList((Integer)index.get(0), (Integer)index.get(1) + 1);
                sylList.add(QuotaFunction.getSyl(sub));
            }
            ksrq = tradingDays.findTradingDay(jsrq, 1);
            //ksrq = tradingDays.findTradingDay(ksrq, 1);
        } while(jsrq <= end);
        return QuotaFunction.tranListToArray(sylList);
    }
    private static List<Integer> getIndexFromCustBills(List<Map<String, Object>> data, int ksrq, int jsrq){
        List<Integer> index = new ArrayList<>();
        for (int i=0; i<data.size(); i++){
            if (Integer.valueOf(data.get(i).get("RQ").toString()).equals(ksrq)){
                index.add(i);
            }
            if (Integer.valueOf(data.get(i).get("RQ").toString()).equals(jsrq)){
                index.add(i);
            }
        }
        return index;
    }
    /**
     * 获得指数收益率
     * @param zsdm
     * @param ksrq
     * @param jsrq
     * @return
     */
    private  static double  getMarketSyl(String zsdm, Integer ksrq, Integer jsrq){
      //  String sqlBuf = "select rq,zxj,zsp from info.this_zshq where zsdm=? and rq=?";
        String sqlBuf = "select rq,zxj,zsp from info.this_zshq where zsdm=? and rq >=? ORDER BY rq ASC LIMIT 1";

        String qcz="";
        String qmz="";
        Properties props = new Properties();
        Connection conn = null;
        ArrayList sylList = new ArrayList();
        try {
            props.load(YearBill_Hengtai.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url").replace("dsc_cfg","info"), props);
            PreparedStatement ps = conn.prepareStatement(sqlBuf);
            ps.setString(1,zsdm);
            ps.setInt(2,ksrq);
            //ps.setInt(2,tradingDays.findTradingDay(ksrq, 1));
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                qcz=  rs.getString(2);
            }
            sqlBuf = "select rq,zxj,zsp from info.this_zshq where zsdm=?  and rq<=? order by rq desc limit 1";
            PreparedStatement psqmz = conn.prepareStatement(sqlBuf);
            psqmz.setString(1,zsdm);
            psqmz.setInt(2,jsrq);
            //psqmz.setInt(2,tradingDays.findTradingDay(jsrq, 0));
            ResultSet rs2 = psqmz.executeQuery();
            while (rs2.next()){
                qmz=  rs2.getString(2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.err.println(e);
                }
            }
        }
        double syl =
                Double.parseDouble(qmz)
                        /Double.valueOf(qcz)-1.0;
        return syl;
    }
    public YearBill_Hengtai() {
        super();
    }
    public class TradingDays{
        private final Logger log = LoggerFactory.getLogger(TradingDays.class);

        //--直接使用int数组，搜索更方便高效
        private int[] arZrr;
        private int[] arJyr;
        private long tmLoad;
        private void initCache(){
            Properties props = new Properties();
            Connection conn = null;
            try {
                props.load(TradingDays.class.getResourceAsStream("/jdbc.properties"));
                Class.forName(props.getProperty("driver"));
                conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url"), props);
                PreparedStatement ps = conn.prepareStatement("select ZRR,JYR from t_xtjyr order by ZRR");
                ResultSet rs = ps.executeQuery();
                List<Integer> lsZrr = new ArrayList<>();
                List<Integer> lsJyr = new ArrayList<>();
                while (rs.next()) {
                    lsZrr.add(rs.getInt(1));
                    lsJyr.add(rs.getInt(2));
                }
                arZrr = Ints.toArray(lsZrr);
                arJyr = Ints.toArray(lsJyr);
                tmLoad = System.currentTimeMillis();
            } catch (Exception e) {
                System.out.println(e.getMessage()+":"+e.toString());
                arZrr = new int[0];
                arJyr = new int[0];
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {

                    }
                }
            }
        }
        //初始化从数据库加载交易日数据
        public  TradingDays(){
            initCache();
        }
        private int findTradingDay(int ndate, int days) {
            int idx = Arrays.binarySearch(arZrr, ndate);
            int len = arZrr.length;
            int n;
            if (idx >= 0 && idx < len) {
                int jyr = arJyr[idx];
                if (ndate != jyr && days < 0){
                    n = (-days) - 1;
                }else{
                    n = (days < 0) ? -days : days;
                }
                while (n != 0) {
                    idx = (days > 0) ? idx + 1 : idx - 1;
                    if (idx < 0 || idx >= len)
                        break;
                    int jyr2 = arJyr[idx];
                    if (jyr != jyr2) {
                        jyr = jyr2;
                        n--;
                    }
                }
                return jyr;
            }
            return ndate;
        }
        private List<Integer> getTradingDays(Integer ksrq, Integer jsrq){
            int[] Zrr = this.arZrr;
            int[] Jyr = this.arJyr;
            List days = new ArrayList();
            int indexStart = Arrays.binarySearch(Zrr, ksrq);
            int indexEnd = Arrays.binarySearch(Zrr, jsrq);
            if (Zrr[indexStart] != Jyr[indexStart]){
                indexStart = Arrays.binarySearch(Zrr, findTradingDay(ksrq, 1));
            }
            for (int idx=indexStart; idx<=indexEnd; idx++){
                if (Zrr[idx] == Jyr[idx]){
                    days.add(Jyr[idx]);
                }
            }
            return days;
        }
    }
}

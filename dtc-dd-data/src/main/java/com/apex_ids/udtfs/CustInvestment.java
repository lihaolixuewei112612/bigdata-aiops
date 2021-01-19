package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.CustInvestmentConf;
import com.apex_ids.util.MyEnum.DailyBillConf;
import com.apex_ids.util.UdfUtil;
import com.apexsoft.quotaMethod.LabelClassfier;
import com.apexsoft.quotaMethod.QuotaFunction;
import com.apexsoft.utils.DateUtils;
import com.google.common.primitives.Ints;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
@Description(
        name = "investment",		   // 风控标签
        value = "select investment(row,20190501,20190531,20180102) row/ksrq/jsrq/initDate"
)
public class CustInvestment extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(CustInvestment.class);
    private static TradingDays tradingDays;
    private static int initDate=20180102;
    private static final String zsdm = "399300";
    private static double noRiskRate =0.04;
    private static int ksrq;
    private static int jsrq;
    private static int qmrq;
    private static double zssyl;
    private static Map<String, String>bd_zsbd;
   // private static double[] zsSylDouble;
    private volatile static HashMap<String,double[]> zsSylDoublehash=new HashMap<>();

    static String result="=";
   // List<Map<String, String>> zsbd;
    private volatile static HashMap<String,List<Map<String, String>>> zsbdhash=new HashMap<>();
    static{
        tradingDays = new TradingDays();
    }
    @Override
    public StructObjectInspector initialize(ObjectInspector[] inOIs) throws UDFArgumentException {
        if (inOIs.length < 1){
            throw new UDFArgumentException("没有数据咯");
        }
        return  putFormat();
    }

    @Override
    public void process(Object[] row) throws HiveException {    //row是传入的参数，f_get_investment函数有4·个函数，分别是row, i_ksrq, i_jsrq, l_initDate
        init(row);
        List<List<Object>> bills = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据
        for (int i=0; i<rows.size(); i++){
            List<String> bill = (List<String>) rows.get(i);//转换
            bills.add(tranDataType(bill));
        }
        sort(bills);//按照日期升序进行排序
        List<Object> monthlyBill = cost(bills);//计算*/
        forward(monthlyBill.toArray());
        /* forward(new ArrayList<>());*/
        /*Object[] objects = new Object[CustInvestmentConf.values().length];
        Arrays.fill(objects, 0D);
        forward(objects);*/
    }
    private List<Object> tranDataType(List<String> bill){
        /**
         * 字段格式标准化
         */
        List<Object> result = new ArrayList<>();
        for (int i=0; i<bill.size(); i++){
            result.add(UdfUtil.parseObject(bill.get(i), DailyBillConf.values()[i].getType()));
        }
        return result;
    }
    @Override
    public void close() throws HiveException {

    }
    private void init(Object[] row){
        result="=";
        ksrq=Integer.valueOf(row[1].toString());
        jsrq=Integer.valueOf(row[2].toString());
        initDate=Integer.valueOf(row[3].toString());
        qmrq = tradingDays.findTradingDay(jsrq, 0);
        if(zsSylDoublehash.get(ksrq+"_"+jsrq)==null){
            //zsSylDouble=new ZsSylMysqlUtil().initCache();
            zsSylDoublehash.put(ksrq+"_"+jsrq,new ZsSylMysqlUtil().initCache());
        }
        if(zsbdhash.get(ksrq+"_"+jsrq+"_"+qmrq)==null){
            zsbdhash.put(ksrq+"_"+jsrq+"_"+qmrq,getIndexFluctuation());
        }
        //zsbd=getIndexFluctuation();
    }
    private void sort(List<List<Object>> bills){
        /**
         * 排序
         */
        Collections.sort(bills,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        Integer d1Str = Integer.valueOf(d1.get(DailyBillConf.rq.getIndex()).toString());
                        Integer d2Str = Integer.valueOf(d2.get(DailyBillConf.rq.getIndex()).toString());
                        return d1Str.compareTo(d2Str);     //按照升序排列
                    }
                });
    }
    private List<Object> cost(List<List<Object>> rows){

        /**
         * 指标计算
         */
        List<List<Object>> bills = new ArrayList<>();
        List<Map<String, Object>> fundList = new ArrayList<>();
        List<Object> custInvestmentQuota = getList();
        for (List<Object> row: rows){
            bills.add(row);
        }
        for (List<Object> bill: rows){
            fundList.add(getCustFund(bill));
        }
        double syl = 0.0d;  //getSyl(fundList);
        try{
            syl= QuotaFunction.getReturnRateByMultiFunc(fundList);
        }catch (Exception e){
            e.printStackTrace();
        }
        double syl_db_dp = syl/zssyl-1.0;
        double[] sylDouble = getSylDoubles(fundList, tradingDays);
        List<String> hc = QuotaFunction.calcZdhc(fundList);
        double bdl = 0.0;
        double sharp = 0.0;
        double beta = 0.0;
        if (sylDouble != null){
            if (sylDouble.length > 0){
                bdl = QuotaFunction.calcBdl(sylDouble);
                sharp = QuotaFunction.calcSharpRatio(sylDouble, noRiskRate);
                //beta = QuotaFunction.calcBeta(sylDouble, zsSylDouble);;
                beta = QuotaFunction.calcBeta(sylDouble, zsSylDoublehash.get(ksrq+"_"+jsrq));;
            }
        }
        double calmar = QuotaFunction.calcCalmar(syl, Double.valueOf(hc.get(0)), DateUtils.getDaysBetween(ksrq, jsrq));

        //List<Double> pmnl = getSituationAbility(fundList, zsbd, tradingDays, initDate);
        List<Double> pmnl = getSituationAbility(fundList, zsbdhash.get(ksrq+"_"+jsrq+"_"+qmrq), tradingDays, initDate);
        custInvestmentQuota.set(CustInvestmentConf.syl.getIndex(),new BigDecimal(syl).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.zdhcl.getIndex(),new BigDecimal(hc.get(0)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.hchf.getIndex(),hc.get(1));
        try{
            custInvestmentQuota.set(CustInvestmentConf.bdl.getIndex(),new BigDecimal(bdl).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }catch (Exception e){
            custInvestmentQuota.set(CustInvestmentConf.bdl.getIndex(),new BigDecimal(0).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }
        try{
            custInvestmentQuota.set(CustInvestmentConf.sharp.getIndex(),new BigDecimal(sharp).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }catch (Exception e){
            custInvestmentQuota.set(CustInvestmentConf.sharp.getIndex(),new BigDecimal(0).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }
        try{
            new BigDecimal(calmar).setScale(6, BigDecimal.ROUND_HALF_UP);       //四舍五入，保留6位小数
        }catch (Exception e){
            calmar=0.0d;
        }
        custInvestmentQuota.set(CustInvestmentConf.calma.getIndex(), new BigDecimal(calmar).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        try{
            new BigDecimal(beta).setScale(6, BigDecimal.ROUND_HALF_UP);
        }catch (Exception e){
            beta=0.0d;
        }
        custInvestmentQuota.set(CustInvestmentConf.beta.getIndex(),new BigDecimal(beta).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.khh.getIndex(), bills.get(0).get(CustInvestmentConf.khh.getIndex()));
        custInvestmentQuota.set(CustInvestmentConf.pmnl_pf.getIndex(),new BigDecimal(pmnl.get(0)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.pmnl_pj.getIndex(),LabelClassfier.getPmnlpj((int) pmnl.get(2).doubleValue(), (int) pmnl.get(3).doubleValue()));
        custInvestmentQuota.set(CustInvestmentConf.pmnl.getIndex(),pmnl.toString());
        //custInvestmentQuota.set(CustInvestmentConf.pmnl_zs.getIndex(),zsbd.toString());
        custInvestmentQuota.set(CustInvestmentConf.pmnl_zs.getIndex(),zsbdhash.get(ksrq+"_"+jsrq+"_"+qmrq).toString());
        custInvestmentQuota.set(CustInvestmentConf.dpsyl.getIndex(),new BigDecimal(zssyl).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        try{
            new BigDecimal(syl_db_dp).setScale(6, BigDecimal.ROUND_HALF_UP);
        }catch (Exception e){
            syl_db_dp=0.0d;
        }
        custInvestmentQuota.set(CustInvestmentConf.syl_db_dp.getIndex(),new BigDecimal(syl_db_dp).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.ylnl_pf.getIndex(),new BigDecimal(LabelClassfier.getYlnlpf(syl_db_dp, syl)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.ylnl_pj.getIndex(),LabelClassfier.getYlnlpj(syl_db_dp));
        custInvestmentQuota.set(CustInvestmentConf.fknl_pf.getIndex(),new BigDecimal(LabelClassfier.getFknlpf(sharp, Double.valueOf(hc.get(0)))).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        custInvestmentQuota.set(CustInvestmentConf.fknl_pj.getIndex(),LabelClassfier.getFknlpj(Double.parseDouble(custInvestmentQuota.get(CustInvestmentConf.fknl_pf.getIndex())+"")));
        custInvestmentQuota.set(CustInvestmentConf.bdl_bq.getIndex(),LabelClassfier.getFkbq("bdl", bdl));
        custInvestmentQuota.set(CustInvestmentConf.hc_bq.getIndex(),LabelClassfier.getFkbq("zdhcl", Double.parseDouble(hc.get(0))));
        return custInvestmentQuota;


    }
    public List<Object> getList(){
        Object[] objects = new Object[CustInvestmentConf.values().length];
        Arrays.fill(objects, 0D);
        return Arrays.asList(objects);
    }


    private static List<Double> getSituationAbility(List<Map<String, Object>> fundList, List<Map<String, String>> zsbd, TradingDays tradingDays, int initDate){
        double pc = 0.0;
        double zdpc = 0.0;//震荡偏差
        double szpc = 0.0;//上涨偏差
        double xjpc = 0.0;//下降偏差
        int zdpj;
        int szpj;
        int xjpj;
        double zpf = 0.0;
        for (int i=0; i<zsbd.size(); i++){
            int ksrq = Integer.parseInt(zsbd.get(i).get("start"));
            int jsrq = Integer.parseInt(zsbd.get(i).get("end"));
            Double syl = QuotaFunction.getSyl(getSubList(fundList, ksrq, jsrq));
            zsbd.get(i).put("syl", syl.toString());
            Double zssyl = Double.parseDouble(zsbd.get(i).get("zssyl"));
            String type = zsbd.get(i).get("type");
            pc = syl - zssyl;
            zsbd.get(i).put("pc", String.valueOf(zsbd.get(i).get("zssyl")));
            if (type.equals("1")){
                szpc += pc;
            }else if (type.equals("-1")){
                xjpc += pc;
            }else{
                zdpc += pc;
            }
        }
        zdpj = LabelClassfier.getStar(zdpc);
        szpj = LabelClassfier.getStar(szpc);
        xjpj = LabelClassfier.getStar(xjpc);
        zpf = (double) (zdpj+szpj+xjpj) * 100d / 9d;
        List<Double> list = new ArrayList<>();
        list.add(zpf);
        list.add((double)zdpj);
        list.add((double)szpj);
        list.add((double)xjpj);
        return list;
    }
    private static List<Map<String, Object>> getSubList(List<Map<String, Object>> fundList, Integer ksrq, Integer jsrq){
        int startIndex = 0;
        int endIndex = fundList.size()-1;
        for (int i=0; i<fundList.size(); i++){
            if (fundList.get(i).get("RQ").toString().equals(ksrq.toString())){
                startIndex = i;
            }
            if (fundList.get(i).get("RQ").toString().equals(jsrq.toString())){
                endIndex = i;
            }
        }
        return fundList.subList(startIndex, endIndex+1);     //截取开始日期到结束日期的数据
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
    private static Map<String, Object> getCustFund(List<Object> bill){
        Map<String, Object> map = new HashMap<>();
        map.put("ZZC", bill.get(DailyBillConf.zzc.getIndex()));
        map.put("ZCJLR", bill.get(DailyBillConf.zcjlr.getIndex()));
        map.put("ZFZ",bill.get(DailyBillConf.zfz.getIndex()));
        map.put("DRYK", bill.get(DailyBillConf.yk.getIndex()));
        map.put("RQ", bill.get(DailyBillConf.rq.getIndex()));
        map.put("ZJYE",bill.get(DailyBillConf.zjye.getIndex()));

        map.put("CRJE",bill.get(DailyBillConf.crje.getIndex()));
        map.put("QCJE",bill.get(DailyBillConf.qcje.getIndex()));
        map.put("ZRZQSZ",bill.get(DailyBillConf.zrzqsz.getIndex()));
        map.put("ZCZQSZ",bill.get(DailyBillConf.zczqsz.getIndex()));
        map.put("QTSR",0);
        map.put("ZXJZ",bill.get(DailyBillConf.zxjz.getIndex()));
        /*map.put("RQ", bill.get(DailyBillConf.rq.getIndex()));
        map.put("ZZC", bill.get(DailyBillConf.zzc.getIndex()));
        map.put("ZCJLR", bill.get(DailyBillConf.zcjlr.getIndex()));
        map.put("ZXJZ", bill.get(DailyBillConf.zxjz.getIndex()));
        map.put("DRYK", bill.get(DailyBillConf.yk.getIndex()));*/
        return map;
    }
    private static List<Map<String, String>> parseIndexFluctuation(String zsbd){
        String pattern = "(\\{.*?\\})";
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(zsbd);
        List<Map<String, String>> data = new ArrayList<>();
        while (matcher.find()){
            data.add(stringToMap(matcher.group().replaceAll(" |'|\\{|\\}", "")));
        }
        return data;
    }
    private static Map<String, String> stringToMap(String s){
        String[] strings = s.split(",");
        Map<String, String> map = new HashMap<>();
        for (String string: strings){
            String[] strings1 = string.split(":");
            if (!strings1[0].contains("value")){
                map.put(strings1[0], strings1[1]);
            }
        }
        return map;
    }
    public List<Map<String, String>>  getIndexFluctuation(){
        String sqlBuf = "select * from info.tfx_zsbd where rq="+jsrq+" and zsdm='"+zsdm+"'";
        result+=sqlBuf;
        Properties props = new Properties();
        Connection conn = null;
        String bd="";
        List<Map<String, String>> zsbd=new ArrayList<>();
        ArrayList sylList = new ArrayList();
        try {
           // props.load(ClassLoader.getSystemResourceAsStream("/jdbc.properties"));
            props.load(TradingDays.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url").replace("dsc_cfg","info"), props);
            PreparedStatement ps = conn.prepareStatement(sqlBuf);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                bd=  rs.getString(3);
            }
            zsbd = parseIndexFluctuation(bd);
            System.out.println(zsbd);
            result+=zsbd;
            if(zsbd.size()==0){
                zsbd.get(0).put("zssyl", "0");
            }
            for (int i=0; i<zsbd.size(); i++){
                Double syl = getMarketSyl(zsdm, Integer.parseInt(zsbd.get(i).get("start")), Integer.parseInt(zsbd.get(i).get("end")));
                zsbd.get(i).put("zssyl", syl.toString());
            }
            zssyl = getMarketSyl(zsdm, ksrq, qmrq);
        } catch (Exception e) {
            System.err.println(e);
            result+=getExceptionAllInfo(e);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    System.err.println(e);
                }
            }
        }
        return zsbd;
    }

    private  double getMarketSyl(String zsdm, Integer ksrq, Integer jsrq){
        String sqlBuf = "select rq,zxj,zsp from info.this_zshq where zsdm=? and rq=?";
        String qcz="";
        String qmz="";
        Properties props = new Properties();
        Connection conn = null;
        ArrayList sylList = new ArrayList();
        try {
            props.load(CustInvestment.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url").replace("dsc_cfg","info"), props);
            PreparedStatement ps = conn.prepareStatement(sqlBuf);
            ps.setString(1,zsdm);
            ps.setInt(2,tradingDays.findTradingDay(ksrq, -1));
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                qcz=  rs.getString(2);
            }
            PreparedStatement psqmz = conn.prepareStatement(sqlBuf);
            psqmz.setString(1,zsdm);
            psqmz.setInt(2,jsrq);
            ResultSet rs2 = psqmz.executeQuery();
            while (rs2.next()){
                qmz=  rs2.getString(2);
            }
        } catch (Exception e) {
            System.err.println(e);
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
        double syl =0.0d;
        try {
            syl= Double.parseDouble(qmz)/Double.valueOf(qcz)-1.0;
        }catch (Exception e){

        }
        return syl;
    }
    public static String getExceptionAllInfo(Exception ex) {
        ByteArrayOutputStream out = null;
        PrintStream pout = null;
        String ret = "";
        try {
            out = new ByteArrayOutputStream();
            pout = new PrintStream(out);
            ex.printStackTrace(pout);
            ret = new String(out.toByteArray());
            out.close();
        }catch(Exception e){
            return ex.getMessage();
        }finally{
            if(pout!=null){
                pout.close();
            }
        }
        return ret;
    }
    public class ZsSylMysqlUtil{
        public double[] initCache(){
            Properties props = new Properties();
            Connection conn = null;
            ArrayList sylList = new ArrayList();
            try {
                props.load(CustInvestment.class.getResourceAsStream("/jdbc.properties"));
                Class.forName(props.getProperty("driver"));
                conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url").replace("dsc_cfg","info"), props);
                int jsrq1;
                int ksrq1=ksrq;
                do {
                    List<Double> queryList = new ArrayList();
                    int pre = tradingDays.findTradingDay(Integer.valueOf(ksrq1), Integer.valueOf(-1));
                    PreparedStatement ps = conn.prepareStatement("select rq,zxj,zsp from info.this_zshq " +
                            "where zsdm='"+zsdm+"' and rq between "+pre+" and "+jsrq+"");
                    System.out.println("select rq,zxj,zsp from info.this_zshq " +
                            "where zsdm='"+zsdm+"' and rq between "+pre+" and "+jsrq+"");
                    ResultSet rs = ps.executeQuery();
                    jsrq1 = DateUtils.getNextMonth(ksrq1, -1);    //下一个月的前一天
                    if(jsrq1 <= jsrq) {
                        while (rs.next()) {
                            queryList.add(rs.getDouble(2));       //遍历第二列的值
                        }
                        sylList.add(queryList.get(queryList.size()-1)/queryList.get(0)-1.0d);
                        ksrq1 = tradingDays.findTradingDay(Integer.valueOf(jsrq1), Integer.valueOf(1));
                    }
                } while(tradingDays.findTradingDay(Integer.valueOf(jsrq1), Integer.valueOf(0)) < jsrq);
                return QuotaFunction.tranListToArray(sylList);
            } catch (Exception e) {
                System.err.println(e);
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
            return new double[0];
        }
    }
    public static class TradingDays{
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
                System.out.println(e.toString()+":"+e.getMessage());
                log.error(e.getMessage());
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
        public TradingDays(){
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
                    if (idx < 0 || idx >= len) {
                        break;
                    }
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
            System.out.println("start:" + indexStart + ";" + "end:" + indexEnd);
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

    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (CustInvestmentConf conf: CustInvestmentConf.values()){
            fieldNames.add(conf.toString());
            outOIS.add(UdfUtil.getInspector(conf.getType()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, outOIS);
        /*ArrayList<String > fieldNames=new ArrayList<String>();
        ArrayList<ObjectInspector> fileois=new ArrayList<ObjectInspector>();
        fieldNames.add("col0");
        fieldNames.add("col1");
        fileois.add(UdfUtil.getInspector("int"));
        fileois.add(UdfUtil.getInspector("string"));
        return  ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fileois);*/
    }
}

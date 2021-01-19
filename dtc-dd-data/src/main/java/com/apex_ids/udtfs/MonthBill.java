package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.DailyBillConf;
import com.apex_ids.util.MyEnum.MonthlyBillConf;
import com.apex_ids.util.UdfUtil;
import com.apexsoft.quotaMethod.QuotaFunction;
import com.google.common.primitives.Ints;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

/**使用方法
 * create function udfs.monthAnalyze as 'com.apex.ids.udtfs.MonthBill' USING JAR 'hdfs://master:8020/user/hive/udfs/udfs-all-1.0.0.jar';
 * select udfs.monthAnalyze(row,20190501,20190531,20180102) from(select khh,udfs.GroupRow(khh,rq,zzc,zzc_jzjy,zzc_rzrq,zzc_ggqq,zjye,zjye_jzjy,zjye_rzrq,zjye_ggqq,zcjlr,crje,qcje,zrzqsz,zczqsz,yk,yk_jzjy,yk_rzrq,yk_jrcp,yk_ggqq,zqsz,zqsz_jzjy,zqsz_rzrq,zqsz_jrcp,zqsz_ggqq,zfz,zfz_rzrq,zxjz,zxjz_jzjy,zxjz_rzrq,zxjz_ggqq) as row from cust.t_stat_zd_r where rq between 20190501 and 20190531 group by khh)a;
 */
public class MonthBill extends GenericUDTF {
    private  int syrq;
    private  int dayNum;
    private  int yearNum;
    private Integer yf;
    private static int initDate=20180102;
    private static TradingDays tradingDays;

    static{
        tradingDays= new TradingDays();
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
        init(row);
        List<List<Object>> bills = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据
        for (int i=0; i<rows.size(); i++){
            List<String> bill = (List<String>) rows.get(i);//转换
            bills.add(tranDataType(bill));
        }
        sort(bills);//排序
        List<Object> monthlyBill = cost(bills);//计算
        forward(monthlyBill.toArray());
    }

    @Override
    public void close() throws HiveException {

    }
    private void init(Object[] row){


            int ksrq = Integer.valueOf(row[1].toString());
            int jsrq = Integer.valueOf(row[2].toString());
            yf = Integer.valueOf(Integer.toString(ksrq).substring(0, 6));
            initDate = Integer.valueOf(row[3].toString());
            dayNum = tradingDays.getTradingDays(ksrq, jsrq).size();
            yearNum = tradingDays.getTradingDays(Integer.valueOf(Integer.toString(ksrq).substring(0, 4) + "0101"),
                    Integer.valueOf(Integer.toString(ksrq).substring(0, 4) + "1231")).size();
            syrq = tradingDays.findTradingDay(ksrq, -1);

    }
    private List<Object> cost(List<List<Object>> bills){
        List<Object> monthlyBill = getList();
        List<Double> zrzcList = new ArrayList<>();
        List<Double> dayList = new ArrayList<>();
        List<Double> jzList = new ArrayList<>();
        List<Double> allDayList = new ArrayList<>();
        double sumZzc = 0D;
        double sumZqsz = 0D;
        int ksrq = (int)bills.get(0).get(DailyBillConf.rq.getIndex());
        boolean idsInitDate = ksrq == initDate;
        boolean duringOpen = (ksrq == syrq || ksrq==initDate) ? false: true;  //false
        boolean forClose = (ksrq == syrq && bills.size()==2) ? true: false;  //false
        int jsrq = (int)bills.get(bills.size()-1).get(DailyBillConf.rq.getIndex());
        List<Map<String, Object>> fundList = new ArrayList<>();

        for(int i=0; i<bills.size(); i++){ //row=bill
            List<Object> bill = bills.get(i);
            jzList.add((double) bill.get(DailyBillConf.zxjz.getIndex()));         //月最新净值列表

            allDayList.add((double)(int)bill.get(DailyBillConf.rq.getIndex()));   //月日期列表
            if (i == 0){
                //期初的信息都是上一月月末一天的数据信息
                monthlyBill.set(MonthlyBillConf.khh.getIndex(), bill.get(DailyBillConf.khh.getIndex()));  //客户号
                monthlyBill.set(MonthlyBillConf.yf.getIndex(), yf);  //月份
                monthlyBill.set(MonthlyBillConf.qczzc.getIndex(),duringOpen ? 0D: bill.get(DailyBillConf.zzc.getIndex())); //起初总资产
                monthlyBill.set(MonthlyBillConf.qczfz.getIndex(),duringOpen ? 0D: bill.get(DailyBillConf.zfz.getIndex())); //期初总负债
                monthlyBill.set(MonthlyBillConf.qczzc_jzjy.getIndex(),duringOpen ? 0D: bill.get(DailyBillConf.zzc_jzjy.getIndex()));//期初总资产_集中交易
                monthlyBill.set(MonthlyBillConf.qczzc_rzrq.getIndex(),duringOpen ? 0D: bill.get(DailyBillConf.zzc_rzrq.getIndex()));//期初总资产_两融
                monthlyBill.set(MonthlyBillConf.qczzc_ggqq.getIndex(),duringOpen ? 0D: bill.get(DailyBillConf.zzc_ggqq.getIndex()));//期初总资产_个股期权
            }
            if ((i == 0 && duringOpen) || i > 0 || idsInitDate){ //非上一日数据
                fundList.add(getCustFund(bill));
                monthlyBill.set(MonthlyBillConf.yk.getIndex(),(double)monthlyBill.get(MonthlyBillConf.yk.getIndex()) + (double)bill.get(DailyBillConf.yk.getIndex()));
                monthlyBill.set(MonthlyBillConf.crje.getIndex(),(double)monthlyBill.get(MonthlyBillConf.crje.getIndex()) + (double)bill.get(DailyBillConf.crje.getIndex()));
                monthlyBill.set(MonthlyBillConf.qcje.getIndex(),(double)monthlyBill.get(MonthlyBillConf.qcje.getIndex()) + (double)bill.get(DailyBillConf.qcje.getIndex()));
                monthlyBill.set(MonthlyBillConf.zrzqsz.getIndex(),(double)monthlyBill.get(MonthlyBillConf.zrzqsz.getIndex()) + (double)bill.get(DailyBillConf.zrzqsz.getIndex()));
                monthlyBill.set(MonthlyBillConf.zczqsz.getIndex(),(double)monthlyBill.get(MonthlyBillConf.zczqsz.getIndex()) + (double)bill.get(DailyBillConf.zczqsz.getIndex()));
                monthlyBill.set(MonthlyBillConf.yk_jzjy.getIndex(),(double)monthlyBill.get(MonthlyBillConf.yk_jzjy.getIndex()) + (double)bill.get(DailyBillConf.yk_jzjy.getIndex()));
                monthlyBill.set(MonthlyBillConf.yk_rzrq.getIndex(),(double)monthlyBill.get(MonthlyBillConf.yk_rzrq.getIndex()) + (double)bill.get(DailyBillConf.yk_rzrq.getIndex()));
                monthlyBill.set(MonthlyBillConf.yk_jrcp.getIndex(),(double)monthlyBill.get(MonthlyBillConf.yk_jrcp.getIndex()) + (double)bill.get(DailyBillConf.yk_jrcp.getIndex()));
                monthlyBill.set(MonthlyBillConf.yk_ggqq.getIndex(),(double)monthlyBill.get(MonthlyBillConf.yk_ggqq.getIndex()) + (double)bill.get(DailyBillConf.yk_ggqq.getIndex()));
                sumZzc = sumZzc + (double)bill.get(DailyBillConf.zzc.getIndex());
                sumZqsz = sumZqsz + (double)bill.get(DailyBillConf.zqsz.getIndex());
                //一般情况，本月的第一天执行该段逻辑
                if ((i == 0 && duringOpen) || (i == 1 && !duringOpen && !idsInitDate) || (i == 0 && idsInitDate)){//期间开户或者前期已开户
                    if (bills.size() != 1){//非最后一天开户
                        if (forClose){
                            zrzcList.add((double)monthlyBill.get(MonthlyBillConf.qczzc.getIndex()) - (double)bill.get(DailyBillConf.zfz.getIndex()));
                        }else {
                            //转入资产列表=期初总资产-每日的负债+每日的资产净流入
                            zrzcList.add((double)monthlyBill.get(MonthlyBillConf.qczzc.getIndex()) - (double)bill.get(DailyBillConf.zfz.getIndex())
                                    +(double)bill.get(DailyBillConf.zcjlr.getIndex()));
                        }

                    }else{//最后一天开户
                        zrzcList.add((double)bill.get(DailyBillConf.zcjlr.getIndex()));
                    }
                    //日期列表
                    dayList.add((double)(int)bill.get(DailyBillConf.rq.getIndex()));
                    ksrq=(int)bill.get(DailyBillConf.rq.getIndex());
                    //TODO:资金净流入
                    //本月除第一天外的转入资金列表
                }else if ((double)bill.get(DailyBillConf.zcjlr.getIndex()) != 0.0 && i < bills.size()-1){
                    zrzcList.add(0.0 - (double)bill.get(DailyBillConf.zcjlr.getIndex()));
                    dayList.add((double)(int)bill.get(DailyBillConf.rq.getIndex()));
                }
                //月末计算逻辑
                if (i == bills.size()-1){
                    monthlyBill.set(MonthlyBillConf.qmzzc.getIndex(),bill.get(DailyBillConf.zzc.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzjye.getIndex(),bill.get(DailyBillConf.zjye.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzqsz.getIndex(),bill.get(DailyBillConf.zqsz.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzfz.getIndex(),bill.get(DailyBillConf.zfz.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzzc_jzjy.getIndex(),bill.get(DailyBillConf.zzc_jzjy.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzqsz_jzjy.getIndex(),bill.get(DailyBillConf.zqsz_jzjy.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzjye_jzjy.getIndex(),bill.get(DailyBillConf.zjye_jzjy.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzxjz_jzjy.getIndex(),bill.get(DailyBillConf.zxjz_jzjy.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzzc_rzrq.getIndex(),bill.get(DailyBillConf.zzc_rzrq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzqsz_rzrq.getIndex(),bill.get(DailyBillConf.zqsz_rzrq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzjye_rzrq.getIndex(),bill.get(DailyBillConf.zjye_rzrq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzfz_rzrq.getIndex(),bill.get(DailyBillConf.zfz_rzrq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzxjz_rzrq.getIndex(),bill.get(DailyBillConf.zxjz_rzrq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzqsz_jrcp.getIndex(),bill.get(DailyBillConf.zqsz_jrcp.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzzc_ggqq.getIndex(),bill.get(DailyBillConf.zzc_ggqq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzjye_ggqq.getIndex(),bill.get(DailyBillConf.zjye_ggqq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzqsz_ggqq.getIndex(),bill.get(DailyBillConf.zqsz_ggqq.getIndex()));
                    monthlyBill.set(MonthlyBillConf.qmzxjz_ggqq.getIndex(),bill.get(DailyBillConf.zxjz_ggqq.getIndex()));

                    if (bills.size() != 1){//非最后一天开户
                        zrzcList.add((double)bill.get(DailyBillConf.zzc.getIndex())-(double)bill.get(DailyBillConf.zcjlr.getIndex()) - (double)bill.get(DailyBillConf.zfz.getIndex()));
                    }else{//最后一天开户
                        zrzcList.add((double)bill.get(DailyBillConf.zzc.getIndex()) - (double)bill.get(DailyBillConf.zfz.getIndex()));
                    }
                    dayList.add((double)(int)bill.get(DailyBillConf.rq.getIndex()));
                    jsrq = (int)bill.get(DailyBillConf.rq.getIndex());
                }
            }
        }
        Integer byts = 0;
        if ((int)bills.get(0).get(DailyBillConf.rq.getIndex())==initDate){
            ksrq = (int)bills.get(0).get(DailyBillConf.rq.getIndex());
        }
        byts=tradingDays.getTradingDays(ksrq, jsrq).size();
        monthlyBill.set(MonthlyBillConf.byts.getIndex(),byts);
        //资金净流入=转入金额+转入证券市值-取出金额-转出证券市值
        monthlyBill.set(MonthlyBillConf.zcjlr.getIndex(),new BigDecimal(monthlyBill.get(MonthlyBillConf.crje.getIndex())+"").
                add(new BigDecimal(monthlyBill.get(MonthlyBillConf.zrzqsz.getIndex())+""))
                .subtract(new BigDecimal(monthlyBill.get(MonthlyBillConf.qcje.getIndex())+"")
                        .add(new BigDecimal(monthlyBill.get(MonthlyBillConf.zczqsz.getIndex())+""))).doubleValue());
        //如果本月的上月最后一天的最新净值为0,那么本月净值周转率也为0
        if( new BigDecimal(bills.get(0).get(DailyBillConf.zxjz.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(MonthlyBillConf.zxjz_zzl.getIndex(),0d);
        }else {
            //最新净值周转率=(最后一天的最新净值-上月最后一天的最新净值)/上月最后一天的最新净值
            monthlyBill.set(MonthlyBillConf.zxjz_zzl.getIndex(), new BigDecimal(bills.get(bills.size()-1).get(DailyBillConf.zxjz.getIndex())+"").
                    subtract(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz.getIndex())+"").
                            divide(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)).doubleValue());
        }
        if( new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_jzjy.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_jzjy.getIndex(),0d);
        }else {
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_jzjy.getIndex(), new BigDecimal(bills.get(bills.size()-1).get(DailyBillConf.zxjz_jzjy.getIndex())+"").
                    subtract(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_jzjy.getIndex())+"").
                            divide(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_jzjy.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)).doubleValue());
        }
        if( new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_rzrq.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_rzrq.getIndex(),0d);
        }else {
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_rzrq.getIndex(), new BigDecimal(bills.get(bills.size()-1).get(DailyBillConf.zxjz_rzrq.getIndex())+"").
                    subtract(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_rzrq.getIndex())+"").
                            divide(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_rzrq.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)).doubleValue());
        }
        if( new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_ggqq.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_ggqq.getIndex(),0d);
        }else {
            monthlyBill.set(MonthlyBillConf.zxjz_zzl_ggqq.getIndex(), new BigDecimal(bills.get(bills.size()-1).get(DailyBillConf.zxjz_ggqq.getIndex())+"").
                    subtract(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_ggqq.getIndex())+"").
                            divide(new BigDecimal(bills.get(0).get(DailyBillConf.zxjz_ggqq.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)).doubleValue());
        }
        try{
            /**
             * 1、正常情况:期初资产=上日资产+当日净流入,期末资产=资产-资金净流入
             * 2、期间开户:期初资产=当日资金净流入,期末资产=资产-资金净流入
             * 3、期间销户:起初资产=上日资产+当日净流入,期末资产=资产-资金净流入
             * 4、最后一天开户:期初资产=当日资金净流入,期末资产=资产
             * 5、第一天销户:期初资产=上日资产,期末资产=资产-资金净流入
             */
            /*monthlyBill.set(MonthlyBillConf.ykl.getIndex(),(new BigDecimal(QuotaFunction.calcSyl
                    (tranListToArray(dayList),
                            tranListToArray(zrzcList),
                            (int)bills.get(0).get(DailyBillConf.rq.getIndex()),
                            (int) bills.get(bills.size()-1).get(DailyBillConf.rq.getIndex())))
                    .setScale(6, BigDecimal.ROUND_HALF_UP)).doubleValue());*/
            /*List<Map<String, Object>> fundList = new ArrayList<>();
            for(List<Object> bill:bills){
                fundList.add(getCustFund(bill));
            }
            Double r=QuotaFunction.getReturnRateByMultiFunc(fundList);
            if(r<-1){
                r=-1.0d;
            }
            monthlyBill.set(MonthlyBillConf.ykl.getIndex(),(new BigDecimal(r)
                    .setScale(6, BigDecimal.ROUND_HALF_UP)).doubleValue());*/
            Double r= QuotaFunction.getReturnRateByMultiFunc(fundList.subList(1,fundList.size()));
            Properties props = new Properties();
            Double sylMin=0.0d;
            Double sylMax=0.0d;
            try {
                props.load(MonthBill.class.getResourceAsStream("/properties/calcSyl.properties"));
                sylMin=Double.parseDouble(props.getProperty("sylMin"));
                sylMax=Double.parseDouble(props.getProperty("sylMax"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            if(r<sylMin){
                r=sylMin;
            }else if(r>sylMax){
                r=sylMax;
            }
            monthlyBill.set(MonthlyBillConf.ykl.getIndex(),(new BigDecimal(r)
                    .setScale(6, BigDecimal.ROUND_HALF_UP)).doubleValue());
            System.out.println(monthlyBill.get(MonthlyBillConf.khh.getIndex()) + "收益率元数据：");
            System.out.println(monthlyBill.get(MonthlyBillConf.khh.getIndex())+":" + dayList + zrzcList);
        }catch (Exception e){
            monthlyBill.set(MonthlyBillConf.ykl.getIndex(),0d);
            System.out.println(monthlyBill.get(MonthlyBillConf.khh.getIndex()) + "收益率元数据：");
            System.out.println(monthlyBill.get(MonthlyBillConf.khh.getIndex()) + ":"+dayList + zrzcList);
            e.printStackTrace();
        }
        monthlyBill.set(MonthlyBillConf.bdl.getIndex(),new BigDecimal(Collections.max(jzList) - Collections.min(jzList)).doubleValue());
        monthlyBill.set(MonthlyBillConf.zdhcl.getIndex(),new BigDecimal(QuotaFunction.calcZdhcl(jzList)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        if (bills.size() == dayNum + 1){
            String s = monthlyBill.get(MonthlyBillConf.ykl.getIndex())+"";
            if(StringUtils.isNotBlank(s)){
                monthlyBill.set(MonthlyBillConf.nhsyl.getIndex(),new BigDecimal((double)monthlyBill.get(MonthlyBillConf.ykl.getIndex())
                        *yearNum/(bills.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
            }
            monthlyBill.set(MonthlyBillConf.pjzzc.getIndex(),new BigDecimal(sumZzc/(bills.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
            monthlyBill.set(MonthlyBillConf.pjsz.getIndex(),new BigDecimal(sumZqsz/(bills.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }else{
            String s = monthlyBill.get(MonthlyBillConf.ykl.getIndex())+"";
            if(StringUtils.isNotBlank(s)){
                monthlyBill.set(MonthlyBillConf.nhsyl.getIndex(),new BigDecimal((double)monthlyBill.get(MonthlyBillConf.ykl.getIndex())*yearNum/bills.size()).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
            }
          //  monthlyBill.set(MonthlyBillConf.nhsyl.getIndex(),new BigDecimal((double)monthlyBill.get(MonthlyBillConf.ykl.getIndex())*yearNum/bills.size()).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
            monthlyBill.set(MonthlyBillConf.pjzzc.getIndex(),new BigDecimal(sumZzc/(bills.size())).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
            monthlyBill.set(MonthlyBillConf.pjsz.getIndex(),new BigDecimal(sumZqsz/(bills.size())).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
        }



        return monthlyBill;
    }
    /*private static Map<String, Object> getCustFund(List<Object> bill){
        Map<String, Object> map = new HashMap<>();
        map.put("RQ", bill.get(DailyBillConf.rq.getIndex()));
        map.put("ZZC", bill.get(DailyBillConf.zzc.getIndex()));
        map.put("ZCJLR", bill.get(DailyBillConf.zcjlr.getIndex()));
        map.put("ZFZ",bill.get(DailyBillConf.zfz.getIndex()));
        map.put("DRYK", bill.get(DailyBillConf.yk.getIndex()));

        map.put("ZJYE",bill.get(DailyBillConf.zjye.getIndex()));
        return map;
    }*/
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
        map.put("QTSR",bill.get(DailyBillConf.qtsr.getIndex()));
        return map;
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
        for (MonthlyBillConf conf: MonthlyBillConf.values()){
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
            result.add(UdfUtil.parseObject(bill.get(i), DailyBillConf.values()[i].getType()));
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
                        Integer d1Str = Integer.valueOf(d1.get(DailyBillConf.rq.getIndex()).toString());
                        Integer d2Str = Integer.valueOf(d2.get(DailyBillConf.rq.getIndex()).toString());
                        return d1Str.compareTo(d2Str);
                    }
                });
    }
    public List<Object> getList(){
        Object[] objects = new Object[MonthlyBillConf.values().length];
        Arrays.fill(objects, 0D);
        return Arrays.asList(objects);
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
                System.out.println("dsc_cfg_url==>"+props.getProperty("dsc_cfg_url"));
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
}

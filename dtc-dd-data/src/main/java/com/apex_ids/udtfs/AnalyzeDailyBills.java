package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.CostConf;
import com.apex_ids.util.MyEnum.CustDailyBillConf;
import com.apex_ids.util.UdfUtil;
import com.apexsoft.quotaMethod.QuotaFunction;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Description(
        name = "AnalyzeDailyBills",
        extended = "资产分析计算迭代"
)
public class AnalyzeDailyBills extends GenericUDTF {
    List<List<Object>> result = new ArrayList<>();//全局变量 放置运算结果
    @Override
    public void process(Object[] row) throws HiveException {
        List<List<Object>> deliveries = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据

        for (int i=0; i<rows.size(); i++){
            List<String> dl = (List<String>) rows.get(i);//转换
            deliveries.add(tranDataType(dl));
        }
        sort(deliveries);//排序
        cost(deliveries);//计算
        for (List<Object> delivery: result){
            forward(delivery.toArray());//输出
        }
    }

    /**
     * 计算
     * @param deliveries
     */
    private void cost(List<List<Object>> deliveries) {
       // List<CustDailyBill> bills = new ArrayList<>();
        //CustMonthlyBill monthlyBill = new CustMonthlyBill();
        List<Object> monthlyBill=new ArrayList<>();
        List<Double> zrzcList = new ArrayList<>();
        List<Double> dayList = new ArrayList<>();
        List<Double> jzList = new ArrayList<>();
        List<Double> allDayList = new ArrayList<>();
        BigDecimal sumZzc = new BigDecimal(0.0).setScale(2);
        BigDecimal sumZqsz = new BigDecimal(0.0).setScale(2);
        int initDate=20170101; //清算日期先写死
        boolean idsInitDate = deliveries.get(0).get(deliveries.get(0).size()-1).toString().equals(initDate);
        int dayNum=20;
        int yearNum=1;
        int tradingDays=20;
        int syrq =20170101;

        int ksrq= (int)deliveries.get(0).get(deliveries.get(0).size()-1);//开始日期
        int jsrq = (int)deliveries.get(deliveries.size()-1).get(deliveries.get(0).size()-1);;//结束日期

        boolean duringOpen = Integer.valueOf(deliveries.get(0).get(deliveries.get(0).size()-1).toString()) == syrq
                || Integer.valueOf(deliveries.get(0).get(deliveries.get(0).size()-1).toString()) ==initDate ? false: true;

        boolean forClose = (Integer.valueOf(deliveries.get(0).get(deliveries.get(0).size()-1).toString())
                == syrq && deliveries.size()==2) ? true: false;


        for(List<Object> bill: deliveries){ //row=bill
           // CustDailyBill bill = bills.get(i);
            jzList.add((Double) bill.get(CustDailyBillConf.Zxjz.getIndex()));
            allDayList.add((Double)bill.get(CustDailyBillConf.Rq.getIndex()));
            int i=0;
            if (i == 0){
               // bill.set(CustDailyBillConf.Khh.getIndex(),bill.get(CustDailyBillConf.Khh.getIndex()));
                monthlyBill.set(CustDailyBillConf.Qczzc.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zzc.getIndex()));
             //   bill.setKhh(bill.getKhh());
             //   monthlyBill.setQczzc(duringOpen ? BigDecimal.ZERO: bill.getZzc());
                monthlyBill.set(CustDailyBillConf.Zzc_jzjy.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zzc_jzjy.getIndex()));
            //    monthlyBill.setQczzc_jzjy(duringOpen ? BigDecimal.ZERO: bill.getZzc_jzjy());
                monthlyBill.set(CustDailyBillConf.Zzc_rzrq.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zzc_rzrq.getIndex()));
             //   monthlyBill.setQczzc_rzrq(duringOpen ? BigDecimal.ZERO: bill.getZzc_rzrq());
                monthlyBill.set(CustDailyBillConf.Zzc_ggqq.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zzc_ggqq.getIndex()));
           //     monthlyBill.setQczzc_ggqq(duringOpen ? BigDecimal.ZERO: bill.getZzc_ggqq());
            }
            if ((i == 0 && duringOpen) || i > 0 || idsInitDate){ //非上一日数据
                monthlyBill.set(CustDailyBillConf.Yk.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Yk.getIndex()));
                monthlyBill.set(CustDailyBillConf.Crje.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Crje.getIndex()));
                monthlyBill.set(CustDailyBillConf.Qcje.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Qcje.getIndex()));
                monthlyBill.set(CustDailyBillConf.Zrzqsz.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zrzqsz.getIndex()));
                monthlyBill.set(CustDailyBillConf.Zrzqsz.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Zrzqsz.getIndex()));
                monthlyBill.set(CustDailyBillConf.Yk_jzjy.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Yk_jzjy.getIndex()));
                monthlyBill.set(CustDailyBillConf.Yk_rzrq.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Yk_rzrq.getIndex()));
                monthlyBill.set(CustDailyBillConf.Yk_jrcp.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Yk_jrcp.getIndex()));
                monthlyBill.set(CustDailyBillConf.Yk_ggqq.getIndex(),duringOpen ? BigDecimal.ZERO: bill.get(CustDailyBillConf.Yk_ggqq.getIndex()));
                sumZzc = sumZzc.add(new BigDecimal(bill.get(CustDailyBillConf.Zzc.getIndex()).toString()));

                /*sumZqsz = sumZqsz.add(bill.getZqsz());
                monthlyBill.setYk(monthlyBill.getYk().add(bill.getYk()));
                monthlyBill.setCrje(monthlyBill.getCrje().add(bill.getCrje()));
                monthlyBill.setQcje(monthlyBill.getQcje().add(bill.getQcje()));
                monthlyBill.setZrzqsz(monthlyBill.getZrzqsz().add(bill.getZrzqsz()));
                monthlyBill.setZczqsz(monthlyBill.getZczqsz().add(bill.getZczqsz()));
                monthlyBill.setYk_jzjy(monthlyBill.getYk_jzjy().add(bill.getYk_jzjy()));
                monthlyBill.setYk_rzrq(monthlyBill.getYk_rzrq().add(bill.getYk_rzrq()));
                monthlyBill.setYk_jrcp(monthlyBill.getYk_jrcp().add(bill.getYk_jrcp()));
                monthlyBill.setYk_ggqq(monthlyBill.getYk_ggqq().add(bill.getYk_ggqq()));
                sumZzc = sumZzc.add(bill.getZzc());
                sumZqsz = sumZqsz.add(bill.getZqsz());*/
                if ((i == 0 && duringOpen) || (i == 1 && !duringOpen && !idsInitDate) || (i == 0 && idsInitDate)){//期间开户或者前期已开户
                    if (deliveries.size() != 1){//非最后一天开户
                        if (forClose){
                            zrzcList.add((double)bill.get(CustDailyBillConf.Qczzc.getIndex()));
                           // zrzcList.add(monthlyBill.getQczzc().doubleValue());
                        }else {
                            zrzcList.add((double)bill.get(CustDailyBillConf.Qczzc.getIndex())
                                    +(double)bill.get(CustDailyBillConf.Zcjlr.getIndex()));
                           // zrzcList.add(monthlyBill.getQczzc().doubleValue() + bill.getZcjlr().doubleValue());
                        }

                    }else{//最后一天开户
                        zrzcList.add((double)bill.get(CustDailyBillConf.Zcjlr.getIndex()));
                        //zrzcList.add(bill.getZcjlr().doubleValue());
                    }
                    dayList.add((double)bill.get(CustDailyBillConf.Rq.getIndex()));
                    //dayList.add(bill.getRq().doubleValue());
                    ksrq=(int)bill.get(CustDailyBillConf.Rq.getIndex());
                   // ksrq = bill.getRq();
            //    }else if (bill.getZcjlr().doubleValue() != 0.0 && i < bills.size()-1){
                }else if ((double)bill.get(CustDailyBillConf.Zcjlr.getIndex()) != 0.0 && i < deliveries.size()-1){
                    /*zrzcList.add(0.0 - bill.getZcjlr().doubleValue());
                    dayList.add(bill.getRq().doubleValue());*/
                    zrzcList.add(0.0 - (double)bill.get(CustDailyBillConf.Zcjlr.getIndex()));
                    dayList.add((double)bill.get(CustDailyBillConf.Rq.getIndex()));
                }
                if (i == deliveries.size()-1){
                    monthlyBill.set(CustDailyBillConf.Qmzzc.getIndex(),bill.get(CustDailyBillConf.Zzc.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzjye.getIndex(),bill.get(CustDailyBillConf.Zjye.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzqsz.getIndex(),bill.get(CustDailyBillConf.Zqsz.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzfz.getIndex(),bill.get(CustDailyBillConf.Zfz.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzzc_jzjy.getIndex(),bill.get(CustDailyBillConf.Zzc_jzjy.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzqsz_jzjy.getIndex(),bill.get(CustDailyBillConf.Zqsz_jzjy.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzjye_jzjy.getIndex(),bill.get(CustDailyBillConf.Zjye_jzjy.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzxjz_jzjy.getIndex(),bill.get(CustDailyBillConf.Zxjz_jzjy.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzzc_rzrq.getIndex(),bill.get(CustDailyBillConf.Zzc_rzrq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzqsz_rzrq.getIndex(),bill.get(CustDailyBillConf.Zqsz_rzrq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzjye_rzrq.getIndex(),bill.get(CustDailyBillConf.Zjye_rzrq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzfz_rzrq.getIndex(),bill.get(CustDailyBillConf.Zfz_rzrq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzxjz_rzrq.getIndex(),bill.get(CustDailyBillConf.Zxjz_rzrq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzqsz_jrcp.getIndex(),bill.get(CustDailyBillConf.Zqsz_jrcp.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzzc_ggqq.getIndex(),bill.get(CustDailyBillConf.Zzc_ggqq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzjye_ggqq.getIndex(),bill.get(CustDailyBillConf.Zjye_ggqq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzqsz_ggqq.getIndex(),bill.get(CustDailyBillConf.Zqsz_ggqq.getIndex()));
                    monthlyBill.set(CustDailyBillConf.Qmzxjz_ggqq.getIndex(),bill.get(CustDailyBillConf.Zxjz_ggqq.getIndex()));

                   /* monthlyBill.setQmzzc(bill.getZzc());
                    monthlyBill.setQmzjye(bill.getZjye());
                    monthlyBill.setQmzqsz(bill.getZqsz());*/
                    /*monthlyBill.setQmzfz(bill.getZfz());
                    monthlyBill.setQmzzc_jzjy(bill.getZzc_jzjy());
                    monthlyBill.setQmzqsz_jzjy(bill.getZqsz_jzjy());
                    monthlyBill.setQmzjye_jzjy(bill.getZjye_jzjy());
                    monthlyBill.setQmzxjz_jzjy(bill.getZxjz_jzjy());
                    monthlyBill.setQmzzc_rzrq(bill.getZzc_rzrq());
                    monthlyBill.setQmzqsz_rzrq(bill.getZqsz_rzrq());
                    monthlyBill.setQmzjye_rzrq(bill.getZjye_rzrq());
                    monthlyBill.setQmzfz_rzrq(bill.getZfz_rzrq());
                    monthlyBill.setQmzxjz_rzrq(bill.getZxjz_rzrq());
                    monthlyBill.setQmzqsz_jrcp(bill.getZqsz_jrcp());
                    monthlyBill.setQmzzc_ggqq(bill.getZzc_ggqq());
                    monthlyBill.setQmzjye_ggqq(bill.getZjye_ggqq());
                    monthlyBill.setQmzqsz_ggqq(bill.getZqsz_ggqq());
                    monthlyBill.setQmzxjz_ggqq(bill.getZxjz_ggqq());*/

                    if (deliveries.size() != 1){//非最后一天开户
                        //zrzcList.add(bill.getZzc().doubleValue() - bill.getZcjlr().doubleValue());
                        zrzcList.add((double)bill.get(CustDailyBillConf.Zzc.getIndex())-(double)bill.get(CustDailyBillConf.Zcjlr.getIndex()));
                    }else{//最后一天开户
                      //  zrzcList.add(bill.getZzc().doubleValue());
                        zrzcList.add((double)bill.get(CustDailyBillConf.Zzc.getIndex()));
                    }
                    dayList.add((double)bill.get(CustDailyBillConf.Rq.getIndex()));
                   // dayList.add(bill.getRq().doubleValue());
                   // jsrq = bill.getRq();
                    jsrq = (int)bill.get(CustDailyBillConf.Rq.getIndex());
                }
                ++i;
            }
        }
        Integer byts = 0;
        if ((int)deliveries.get(0).get(CustDailyBillConf.Rq.getIndex())==initDate){
           // ksrq = deliveries.get(0).getRq();
            ksrq = (int)deliveries.get(0).get(CustDailyBillConf.Rq.getIndex());
        }
       // byts = tradingDays.getTradingDays(ksrq, jsrq).size();
        byts=tradingDays;
        monthlyBill.set(CustDailyBillConf.Byts.getIndex(),byts);
        monthlyBill.set(CustDailyBillConf.Zcjlr.getIndex(),new BigDecimal(monthlyBill.get(CustDailyBillConf.Crje.getIndex())+"").
                add(new BigDecimal(monthlyBill.get(CustDailyBillConf.Zrzqsz.getIndex())+""))
                     .subtract(new BigDecimal(monthlyBill.get(CustDailyBillConf.Qcje.getIndex())+"")
                        .add(new BigDecimal(monthlyBill.get(CustDailyBillConf.Zczqsz.getIndex())+""))));

        /*monthlyBill.setZcjlr((monthlyBill.getCrje().add(monthlyBill.getZrzqsz())).subtract(
                monthlyBill.getQcje().add(monthlyBill.getZczqsz())
        ));*/
        monthlyBill.set(CustDailyBillConf.Zcjlr.getIndex(),new BigDecimal(monthlyBill.get(CustDailyBillConf.Crje.getIndex())+"").
                add(new BigDecimal(monthlyBill.get(CustDailyBillConf.Zrzqsz.getIndex())+""))
                .subtract(new BigDecimal(monthlyBill.get(CustDailyBillConf.Qcje.getIndex())+"")
                        .add(new BigDecimal(monthlyBill.get(CustDailyBillConf.Zczqsz.getIndex())+""))));



        /*monthlyBill.set(CustDailyBillConf.Zxjz_zzl.getIndex(),new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz.getIndex())+"")
                .compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : deliveries.get(bills.size()-1).getZxjz().subtract(deliveries.get(0).
                get(CustDailyBillConf.Zxjz.getIndex())).divide(deliveries.get(0).get(CustDailyBillConf.Zxjz.getIndex()), 6,
                BigDecimal.ROUND_HALF_UP));*/

        if( new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl.getIndex(),BigDecimal.ZERO);
        }else {
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl.getIndex(), new BigDecimal(deliveries.get(deliveries.size()-1).get(CustDailyBillConf.Zxjz.getIndex())+"").
                    subtract(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz.getIndex())+"").
                            divide(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)));
        }
       // monthlyBill.setZxjz_zzl(deliveries.get(0).getZxjz().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : deliveries.get(bills.size()-1).getZxjz().subtract(deliveries.get(0).getZxjz()).divide(deliveries.get(0).getZxjz(), 6, BigDecimal.ROUND_HALF_UP));

        if( new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_jzjy.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_jzjy.getIndex(),BigDecimal.ZERO);
        }else {
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_jzjy.getIndex(), new BigDecimal(deliveries.get(deliveries.size()-1).get(CustDailyBillConf.Zxjz_jzjy.getIndex())+"").
                    subtract(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_jzjy.getIndex())+"").
                    divide(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_jzjy.getIndex())+""),
                            6, BigDecimal.ROUND_HALF_UP)));
        }

        //monthlyBill.setZxjz_zzl_jzjy(deliveries.get(0).getZxjz_jzjy().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : deliveries.get(deliveries.size()-1).getZxjz_jzjy().subtract(deliveries.get(0).getZxjz_jzjy()).divide(deliveries.get(0).getZxjz_jzjy(), 6, BigDecimal.ROUND_HALF_UP));
        if( new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_rzrq.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_rzrq.getIndex(),BigDecimal.ZERO);
        }else {
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_rzrq.getIndex(), new BigDecimal(deliveries.get(deliveries.size()-1).get(CustDailyBillConf.Zxjz_rzrq.getIndex())+"").
                    subtract(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_rzrq.getIndex())+"").
                            divide(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_rzrq.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)));
        }
       // monthlyBill.setZxjz_zzl_rzrq(deliveries.get(0).getZxjz_rzrq().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : deliveries.get(deliveries.size()-1).getZxjz_rzrq().subtract(deliveries.get(0).getZxjz_rzrq()).divide(deliveries.get(0).getZxjz_rzrq(), 6, BigDecimal.ROUND_HALF_UP));
        if( new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_ggqq.getIndex())+"").compareTo(BigDecimal.ZERO) == 0){
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_ggqq.getIndex(),BigDecimal.ZERO);
        }else {
            monthlyBill.set(CustDailyBillConf.Zxjz_zzl_ggqq.getIndex(), new BigDecimal(deliveries.get(deliveries.size()-1).get(CustDailyBillConf.Zxjz_ggqq.getIndex())+"").
                    subtract(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_ggqq.getIndex())+"").
                            divide(new BigDecimal(deliveries.get(0).get(CustDailyBillConf.Zxjz_ggqq.getIndex())+""),
                                    6, BigDecimal.ROUND_HALF_UP)));
        }
      //  monthlyBill.setZxjz_zzl_ggqq(deliveries.get(0).getZxjz_ggqq().compareTo(BigDecimal.ZERO) == 0 ? BigDecimal.ZERO : deliveries.get(deliveries.size()-1).getZxjz_ggqq().subtract(deliveries.get(0).getZxjz_ggqq()).divide(deliveries.get(0).getZxjz_ggqq(), 6, BigDecimal.ROUND_HALF_UP));
        try{
            /**
             * 1、正常情况:期初资产=上日资产+当日净流入,期末资产=资产-资金净流入
             * 2、期间开户:期初资产=当日资金净流入,期末资产=资产-资金净流入
             * 3、期间销户:起初资产=上日资产+当日净流入,期末资产=资产-资金净流入
             * 4、最后一天开户:期初资产=当日资金净流入,期末资产=资产
             * 5、第一天销户:期初资产=上日资产,期末资产=资产-资金净流入
             */
            monthlyBill.set(CustDailyBillConf.Ykl.getIndex(),(new BigDecimal(QuotaFunction.calcSyl
                    (tranListToArray(dayList),
                    tranListToArray(zrzcList),
                            (int)deliveries.get(0).get(CustDailyBillConf.Rq.getIndex()),
                            (int) deliveries.get(deliveries.size()-1).get(CustDailyBillConf.Rq.getIndex())))
                    .setScale(6, BigDecimal.ROUND_HALF_UP)));
            System.out.println(monthlyBill.get(CustDailyBillConf.Khh.getIndex()) + "收益率元数据：");
            System.out.println(monthlyBill.get(CustDailyBillConf.Khh.getIndex())+":" + dayList + zrzcList);
        }catch (Exception e){
            monthlyBill.set(CustDailyBillConf.Ykl.getIndex(),BigDecimal.ZERO);
            System.out.println(monthlyBill.get(CustDailyBillConf.Khh.getIndex()) + "收益率元数据：");
            System.out.println(monthlyBill.get(CustDailyBillConf.Khh.getIndex()) + ":"+dayList + zrzcList);
            e.printStackTrace();
        }
        monthlyBill.set(CustDailyBillConf.Bdl.getIndex(),new BigDecimal(Collections.max(jzList) - Collections.min(jzList)));
        monthlyBill.set(CustDailyBillConf.Zdhcl.getIndex(),new BigDecimal(QuotaFunction.calcZdhcl(jzList)).setScale(6, BigDecimal.ROUND_HALF_UP));
        if (deliveries.size() == dayNum + 1){
            monthlyBill.set(CustDailyBillConf.Nhsyl.getIndex(),new BigDecimal((double)monthlyBill.get(CustDailyBillConf.Ykl.getIndex())*yearNum/(deliveries.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP));
            monthlyBill.set(CustDailyBillConf.Pjzzc.getIndex(),new BigDecimal(sumZzc.doubleValue()/(deliveries.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP));
            monthlyBill.set(CustDailyBillConf.Pjsz.getIndex(),new BigDecimal(sumZqsz.doubleValue()/(deliveries.size()-1)).setScale(6, BigDecimal.ROUND_HALF_UP));
        }else{
            monthlyBill.set(CustDailyBillConf.Nhsyl.getIndex(),new BigDecimal((double)monthlyBill.get(CustDailyBillConf.Ykl.getIndex())*yearNum/deliveries.size()).setScale(6, BigDecimal.ROUND_HALF_UP));
            monthlyBill.set(CustDailyBillConf.Pjzzc.getIndex(),new BigDecimal(sumZzc.doubleValue()/(deliveries.size())).setScale(6, BigDecimal.ROUND_HALF_UP));
            monthlyBill.set(CustDailyBillConf.Pjsz.getIndex(),new BigDecimal(sumZqsz.doubleValue()/(deliveries.size())).setScale(6, BigDecimal.ROUND_HALF_UP));
        }
        result.add(monthlyBill);
    }

    private static double[] tranListToArray(List<Double> data){
        double[] tmp = new double[data.size()];
        for (int i=0; i<data.size(); i++){
            tmp[i] = data.get(i);
        }
        return tmp;
    }

    /**
     * 排序
     */
    private void sort(List<List<Object>> deliveries) {
        Collections.sort(deliveries,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        try {
                            String d1s=d1.get(CustDailyBillConf.Rq.getIndex()).toString();
                            String d2s=d2.get(CustDailyBillConf.Rq.getIndex()).toString();
                           // String d1Str = d1.get(CostConf.gdh.getIndex()).toString() + d1.get(CostConf.jys.getIndex()) + d1.get(CostConf.zqdm.getIndex());
                           // String d2Str = d2.get(CostConf.gdh.getIndex()).toString() + d2.get(CostConf.jys.getIndex()) + d2.get(CostConf.zqdm.getIndex());
                            int flag = d1s.compareToIgnoreCase(d2s);
                            if (flag == 0) {
                                return Long.valueOf(d1.get(CustDailyBillConf.Rq.getIndex()).toString()).compareTo(Long.valueOf(d2.get(CustDailyBillConf.Rq.getIndex()).toString()));
                            } else {
                                return flag;
                            }
                        } catch (Exception e) {

                        }
                        return 1;
                    }
                });
    }
    @Override
    public void close() throws HiveException {

    }
    private List<Object> tranDataType(List<String> delivery){
        /**
         * 字段格式标准化
         */
        List<Object> result = new ArrayList<>();
        for (int i=0; i<delivery.size(); i++){
            result.add(UdfUtil.parseObject(delivery.get(i), CostConf.values()[i].getType()));
        }
        return result;
    }
}

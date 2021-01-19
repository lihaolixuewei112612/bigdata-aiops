package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.FundConf;
import com.apex_ids.util.UdfUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

@Description(
        name = "ClassFund",
        extended = "分级基金合并拆分"
)
public class ClassFund extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] inOIs) throws UDFArgumentException {
        if (inOIs.length < 1){
            throw new UDFArgumentException("没有数据咯");
        }
        return putFormat();
    }

    @Override
    public void process(Object[] row) throws HiveException {
        List<List<Object>> deliveries = new ArrayList<>();
        List rows = UdfUtil.getList(row[0]);//获取数据
        for (int i=0; i<rows.size(); i++){
            List<String> dl = (List<String>) rows.get(i);
            deliveries.add(tranDataType(dl));//转换
        }
        sort(deliveries);//排序
        cost(deliveries);//计算
        for (List<Object> delivery: deliveries){
            forward(delivery.toArray());//输出
        }
    }

    @Override
    public void close() throws HiveException {

    }
    private void cost(List<List<Object>> deliveries){
        String cust_sec = "";
        int seqno_qs_pre = 0;
        String jylb_qs_pre = "0";
        String jylb_qs = "";
        double jyfy_cur;
        String jylb_qs_cur, cjbh_cur;
        List<Object> pre = new ArrayList<>();
        int flag;
//        int num = 0;
        for (List<Object> row: deliveries){
            try{
            if (!StringUtils.isBlank(row.get(FundConf.jylb_qs.getIndex()).toString())){
                String cust_sec_cur = row.get(FundConf.khh.getIndex()).toString() + row.get(FundConf.gdh.getIndex()) + row.get(FundConf.jys.getIndex()) + row.get(FundConf.zqdm_fjmj.getIndex());
                if (!cust_sec.equals(cust_sec_cur)) {
                    cust_sec = cust_sec_cur;
                    seqno_qs_pre = 0;
                    jylb_qs_pre = "0";
                    pre = getFirstRow(
                            row.get(FundConf.gdh.getIndex()).toString(),
                            row.get(FundConf.jys.getIndex()).toString(),
                            row.get(FundConf.zqdm_fjmj.getIndex()).toString(),
                            seqno_qs_pre,
                            jylb_qs_pre,
                            deliveries);
                    if (pre == null){
                        row.set(FundConf.remark.getIndex(), "未获取到先前记录");
                    }
                }
                flag = Integer.valueOf(row.get(FundConf.flag.getIndex()).toString()).intValue();
                if (flag != 0){
                    jylb_qs_cur = row.get(FundConf.jylb_qs.getIndex()).toString();
                    cjbh_cur = row.get(FundConf.cjbh.getIndex()).toString();
                    jyfy_cur = Double.valueOf(row.get(FundConf.jyfy.getIndex()).toString()).doubleValue();
                    if (Constants.jylbMR.contains(jylb_qs_cur) || (Constants.jylbSG.contains(jylb_qs_cur) && !Constants.CJBH_SGFQWT.equals(cjbh_cur)) || (Constants.JYLB_TGZR.equals(jylb_qs_cur) && Constants.CJBH_FEBD.equals(cjbh_cur))) {
                        // 发生买入交易行为（1买入,16送股,61融资买入,41认购,42申购,59大宗买入,60大宗卖出,80申保,83中签）
                        jylb_qs = Constants.JYLB_BUY;
                    } else if (Constants.jylbSY.contains(jylb_qs_cur) || (Constants.JYLB_JJSH.equals(jylb_qs_cur) && !Constants.cjbhList.contains(cjbh_cur))) {
                        // 发生卖出交易行为（2卖出,6分红,17兑付,22兑息,43赎回,45分红,71融资强平,81退保）
                        jylb_qs = Constants.JYLB_SELL;
                    } else {
                        jylb_qs = jylb_qs_cur;
                        jyfy_cur = 0;
                    }
                    if (flag == 1){
                        if (jylb_qs.equals("1")) {
                            row.set(FundConf.mjdm_bcye.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) + (double) row.get(FundConf.mjdm_yssl.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), new BigDecimal((double) pre.get(FundConf.mjdm_cccb.getIndex()) + Math.abs((double) row.get(FundConf.mjdm_ysje.getIndex()))).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_cbj.getIndex(), (double) row.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d :
                                    new BigDecimal((double)row.get(FundConf.mjdm_cccb.getIndex())/(double)row.get(FundConf.mjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_ljyk.getIndex(), pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股" + row.get(FundConf.mjdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.mjdm_cjjg.getIndex()) + ";买入前成本价:" + new BigDecimal(pre.get(FundConf.mjdm_cbj.getIndex()).toString()).setScale(4, BigDecimal.ROUND_HALF_UP) + ";买入后成本价:" + new BigDecimal(row.get(FundConf.mjdm_cbj.getIndex()).toString()).setScale(4, BigDecimal.ROUND_HALF_UP));
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else if (jylb_qs.equals("2")) {
                            row.set(FundConf.mjdm_bcye.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) + (double)row.get(FundConf.mjdm_yssl.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), new BigDecimal((double)pre.get(FundConf.mjdm_cbj.getIndex()) * (double)row.get(FundConf.mjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_cbj.getIndex(), (double)row.get(FundConf.mjdm_cccb.getIndex()) == 0d ? 0d : pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_dcyk.getIndex(), new BigDecimal(Math.abs((double)row.get(FundConf.mjdm_ysje.getIndex())) - (double)pre.get(FundConf.mjdm_cbj.getIndex())*Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex()))).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_ljyk.getIndex()) + (double)row.get(FundConf.mjdm_dcyk.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股:" + row.get(FundConf.mjdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.mjdm_cjjg.getIndex()) + ";成本价:" + new BigDecimal(pre.get(FundConf.mjdm_cbj.getIndex()).toString()).setScale(4, BigDecimal.ROUND_HALF_UP) + ";盈利:" + row.get(FundConf.mjdm_dcyk.getIndex()));
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else {
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_dcyk.getIndex(), 0d);
                            row.set(FundConf.mjdm_ljyk.getIndex(), pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHFQWT)){
                                row.set(FundConf.remark.getIndex(), "发起赎回委托");
                            }else if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHJSCG)){
                                row.set(FundConf.remark.getIndex(), "赎回到期交收");
                            }
                        }
                    }else if (flag == 2){
                        if (jylb_qs.equals("1")) {
                            row.set(FundConf.ajdm_bcye.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) + Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex())));
                            row.set(FundConf.ajdm_cccb.getIndex(), new BigDecimal((double)pre.get(FundConf.ajdm_cccb.getIndex()) + Math.abs((double)row.get(FundConf.ajdm_ysje.getIndex()))).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_cbj.getIndex(), (double) row.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d :
                                    new BigDecimal((double)row.get(FundConf.ajdm_cccb.getIndex())/(double)row.get(FundConf.ajdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0d : pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股:" + row.get(FundConf.ajdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.ajdm_cjjg.getIndex()) + ";买入前成本价:" + new BigDecimal(pre.get(FundConf.ajdm_cbj.getIndex()).toString()).setScale(4,BigDecimal.ROUND_HALF_UP) + ";买入后成本价:" + new BigDecimal(row.get(FundConf.ajdm_cbj.getIndex()).toString()).setScale(4,BigDecimal.ROUND_HALF_UP));

                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else if (jylb_qs.equals("2")) {
                            row.set(FundConf.ajdm_bcye.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) + (double)row.get(FundConf.ajdm_yssl.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), new BigDecimal((double)pre.get(FundConf.ajdm_cbj.getIndex()) * (double)row.get(FundConf.ajdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_cbj.getIndex(), (double)row.get(FundConf.ajdm_cccb.getIndex()) == 0d ? 0d : pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_dcyk.getIndex(), new BigDecimal(Math.abs((double)row.get(FundConf.ajdm_ysje.getIndex())) - (double)pre.get(FundConf.ajdm_cbj.getIndex())*Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex()))).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_ljyk.getIndex(), (double)pre.get(FundConf.ajdm_ljyk.getIndex()) + (double)row.get(FundConf.ajdm_dcyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股:" + row.get(FundConf.ajdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.ajdm_cjjg.getIndex()) + ";成本价:" + new BigDecimal(pre.get(FundConf.ajdm_cbj.getIndex()).toString()).setScale(4, BigDecimal.ROUND_HALF_UP) + ";盈利:" + row.get(FundConf.ajdm_dcyk.getIndex()));
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else {
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_dcyk.getIndex(), 0d);
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHFQWT)){
                                row.set(FundConf.remark.getIndex(), "发起赎回委托");
                            }else if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHJSCG)){
                                row.set(FundConf.remark.getIndex(), "赎回到期交收");
                            }
                        }
                    }else if (flag == 4){
                        if (jylb_qs.equals("1")) {
                            row.set(FundConf.bjdm_bcye.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) + (double)row.get(FundConf.bjdm_yssl.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), new BigDecimal((double)pre.get(FundConf.bjdm_cccb.getIndex()) + Math.abs((double)row.get(FundConf.bjdm_ysje.getIndex()))).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_cbj.getIndex(), (double) row.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0d :
                                    new BigDecimal((double)row.get(FundConf.bjdm_cccb.getIndex())/(double)row.get(FundConf.bjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d : pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股:" + row.get(FundConf.bjdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.bjdm_cjjg.getIndex()) + ";买入前成本价:" + new BigDecimal(pre.get(FundConf.bjdm_cbj.getIndex()).toString()).setScale(4,BigDecimal.ROUND_HALF_UP) + ";买入后成本价:" + new BigDecimal(row.get(FundConf.bjdm_cbj.getIndex()).toString()).setScale(4,BigDecimal.ROUND_HALF_UP));
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else if (jylb_qs.equals("2")) {
                            row.set(FundConf.bjdm_bcye.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) + (double)row.get(FundConf.bjdm_yssl.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), new BigDecimal((double)pre.get(FundConf.bjdm_cbj.getIndex()) * (double)row.get(FundConf.bjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_cbj.getIndex(), (double)row.get(FundConf.bjdm_cccb.getIndex()) == 0d ? 0d : pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_dcyk.getIndex(), new BigDecimal(Math.abs((double)row.get(FundConf.bjdm_ysje.getIndex())) - (double)pre.get(FundConf.bjdm_cbj.getIndex())*Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex()))).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_ljyk.getIndex(), (double)pre.get(FundConf.bjdm_ljyk.getIndex()) + (double)row.get(FundConf.bjdm_dcyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_cccb.getIndex()) == 0d ? 0d : pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), (double)pre.get(FundConf.ajdm_cccb.getIndex()) == 0d ? 0d : pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "本次交易后持股:" + row.get(FundConf.bjdm_bcye.getIndex()) + ";成交价:" + row.get(FundConf.bjdm_cjjg.getIndex()) + ";成本价:" + new BigDecimal(pre.get(FundConf.bjdm_cbj.getIndex()).toString()).setScale(4, BigDecimal.ROUND_HALF_UP) + ";盈利:" + row.get(FundConf.bjdm_dcyk.getIndex()));
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                        } else {
                            row.set(FundConf.bjdm_bcye.getIndex(), pre.get(FundConf.bjdm_bcye.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), pre.get(FundConf.bjdm_cccb.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), pre.get(FundConf.bjdm_cbj.getIndex()));
                            row.set(FundConf.bjdm_dcyk.getIndex(), 0d);
                            row.set(FundConf.bjdm_ljyk.getIndex(), pre.get(FundConf.bjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), pre.get(FundConf.mjdm_cccb.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), pre.get(FundConf.mjdm_cbj.getIndex()));
                            row.set(FundConf.mjdm_ljyk.getIndex(), pre.get(FundConf.mjdm_ljyk.getIndex()));
                            row.set(FundConf.mjdm_bcye.getIndex(), pre.get(FundConf.mjdm_bcye.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), pre.get(FundConf.ajdm_cccb.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), pre.get(FundConf.ajdm_cbj.getIndex()));
                            row.set(FundConf.ajdm_ljyk.getIndex(), pre.get(FundConf.ajdm_ljyk.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), pre.get(FundConf.ajdm_bcye.getIndex()));
                            row.set(FundConf.jyfy.getIndex(), jyfy_cur);
                            if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHFQWT)){
                                row.set(FundConf.remark.getIndex(), "发起赎回委托");
                            }else if (jylb_qs_cur.equals(Constants.JYLB_JJSH) && cjbh_cur.equals(Constants.CJBH_SHJSCG)){
                                row.set(FundConf.remark.getIndex(), "赎回到期交收");
                            }
                        }
                    }else {
                        if (jylb_qs.equals("47")) {
                            row.set(FundConf.mjdm_bcye.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) + (double)row.get(FundConf.mjdm_yssl.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) + (double)row.get(FundConf.ajdm_yssl.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) + (double)row.get(FundConf.bjdm_yssl.getIndex()));
                            row.set(FundConf.cccb_zy.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.mjdm_cccb.getIndex()) * Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex())) / (double)pre.get(FundConf.mjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ljyk_zy.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.mjdm_ljyk.getIndex()) * Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex())) / (double)pre.get(FundConf.mjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.cccb_zy_ahm.getIndex(), Math.abs((double)row.get(FundConf.ajdm_ysje.getIndex())));
                            row.set(FundConf.ljyk_zy_ahm.getIndex(), (double)row.get(FundConf.cccb_zy.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.ljyk_zy.getIndex()) * Math.abs((double)row.get(FundConf.ajdm_ysje.getIndex()) / (double)row.get(FundConf.cccb_zy.getIndex()))).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.cccb_zy_bhm.getIndex(), (double)row.get(FundConf.cccb_zy.getIndex()) - Math.abs((double)row.get(FundConf.ajdm_ysje.getIndex())));
                            row.set(FundConf.ljyk_zy_ahm.getIndex(), (double)row.get(FundConf.cccb_zy.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.ljyk_zy.getIndex()) - (double)row.get(FundConf.ljyk_zy_ahm.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_cccb.getIndex(), (double)pre.get(FundConf.mjdm_cccb.getIndex()) - (double)row.get(FundConf.cccb_zy.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), (double)row.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.mjdm_cccb.getIndex()) / (double)row.get(FundConf.mjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_ljyk.getIndex()) - (double)row.get(FundConf.ljyk_zy.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), (double)pre.get(FundConf.ajdm_cccb.getIndex()) + (double)row.get(FundConf.cccb_zy_ahm.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), (double)row.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.ajdm_cccb.getIndex()) / (double)row.get(FundConf.ajdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_ljyk.getIndex(), (double)pre.get(FundConf.ajdm_ljyk.getIndex()) + (double)row.get(FundConf.ljyk_zy_ahm.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), (double)pre.get(FundConf.bjdm_cccb.getIndex()) + (double)row.get(FundConf.cccb_zy_bhm.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), (double)row.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.bjdm_cccb.getIndex()) / (double)row.get(FundConf.bjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_ljyk.getIndex(), (double)pre.get(FundConf.bjdm_ljyk.getIndex()) + (double)row.get(FundConf.ljyk_zy_bhm.getIndex()));
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "分级基金拆分：母基金(拆分数量:" + Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex())) + ";拆分成本价:" + new BigDecimal((double)row.get(FundConf.cccb_zy.getIndex()) / Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue() + ")。" + "其中：A基金(拆分获得数量:" + row.get(FundConf.ajdm_yssl.getIndex()) + ";成本价:" + new BigDecimal((double)row.get(FundConf.cccb_zy_ahm.getIndex()) / Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP) + ");B基金(拆分获得数量:" + Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex())) + ";成本价:" + new BigDecimal((double)row.get(FundConf.cccb_zy_bhm.getIndex()) / Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP) + ")");
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                            row.set(FundConf.mjdm_cjjg.getIndex(), new BigDecimal((double)row.get(FundConf.cccb_zy.getIndex()) / Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_cjjg.getIndex(), new BigDecimal((double)row.get(FundConf.cccb_zy_ahm.getIndex()) / Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_cjjg.getIndex(), new BigDecimal((double)row.get(FundConf.cccb_zy_bhm.getIndex()) / Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue());
                        } else if (jylb_qs.equals("48")) {
                            row.set(FundConf.mjdm_bcye.getIndex(), (double)pre.get(FundConf.mjdm_bcye.getIndex()) + (double)row.get(FundConf.mjdm_yssl.getIndex()));
                            row.set(FundConf.ajdm_bcye.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) + (double)row.get(FundConf.ajdm_yssl.getIndex()));
                            row.set(FundConf.bjdm_bcye.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) + (double)row.get(FundConf.bjdm_yssl.getIndex()));
                            row.set(FundConf.cccb_zy_ahm.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.ajdm_cccb.getIndex()) * Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex())) / (double)pre.get(FundConf.ajdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ljyk_zy_ahm.getIndex(), (double)pre.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.ajdm_ljyk.getIndex()) * Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex())) / (double)pre.get(FundConf.ajdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.cccb_zy_bhm.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.bjdm_cccb.getIndex()) * Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex())) / (double)pre.get(FundConf.bjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ljyk_zy_bhm.getIndex(), (double)pre.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)pre.get(FundConf.bjdm_ljyk.getIndex()) * Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex())) / (double)pre.get(FundConf.bjdm_bcye.getIndex())).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.cccb_zy.getIndex(), (double)row.get(FundConf.cccb_zy_ahm.getIndex()) + (double)row.get(FundConf.cccb_zy_bhm.getIndex()));
                            row.set(FundConf.ljyk_zy.getIndex(), (double)row.get(FundConf.ljyk_zy_ahm.getIndex()) + (double)row.get(FundConf.ljyk_zy_bhm.getIndex()));
                            row.set(FundConf.mjdm_cccb.getIndex(), (double)pre.get(FundConf.mjdm_cccb.getIndex()) + (double)row.get(FundConf.cccb_zy.getIndex()));
                            row.set(FundConf.mjdm_cbj.getIndex(), (double)row.get(FundConf.mjdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.mjdm_cccb.getIndex()) / (double)row.get(FundConf.mjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.mjdm_ljyk.getIndex(), (double)pre.get(FundConf.mjdm_ljyk.getIndex()) + (double)row.get(FundConf.ljyk_zy.getIndex()));
                            row.set(FundConf.ajdm_cccb.getIndex(), (double)pre.get(FundConf.ajdm_cccb.getIndex()) - (double)row.get(FundConf.cccb_zy_ahm.getIndex()));
                            row.set(FundConf.ajdm_cbj.getIndex(), (double)row.get(FundConf.ajdm_bcye.getIndex()) == 0d ? 0d : new BigDecimal((double)row.get(FundConf.ajdm_cccb.getIndex()) / (double)row.get(FundConf.ajdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_ljyk.getIndex(), (double)pre.get(FundConf.ajdm_ljyk.getIndex()) - (double)row.get(FundConf.ljyk_zy_ahm.getIndex()));
                            row.set(FundConf.bjdm_cccb.getIndex(), (double)pre.get(FundConf.bjdm_cccb.getIndex()) - (double)row.get(FundConf.cccb_zy_bhm.getIndex()));
                            row.set(FundConf.bjdm_cbj.getIndex(), (double)row.get(FundConf.bjdm_bcye.getIndex()) == 0d ? 0 : new BigDecimal((double)row.get(FundConf.bjdm_cccb.getIndex()) / (double)row.get(FundConf.bjdm_bcye.getIndex())).setScale(7, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_ljyk.getIndex(), (double)pre.get(FundConf.bjdm_ljyk.getIndex()) - (double)row.get(FundConf.ljyk_zy_bhm.getIndex()));
                            try{
                                row.set(FundConf.remark.getIndex(),
                                        "分级基金合并：母基金(合并所得数量:" + Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex())) + ";合并成本价:" + new BigDecimal((double)row.get(FundConf.cccb_zy.getIndex()) / Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex()))).setScale(4, BigDecimal.ROUND_HALF_UP) + ")。" + "其中：A基金(合并数量:" + Math.abs((double)row.get(FundConf.ajdm_yssl.getIndex())) + ";成本价:" + new BigDecimal((double)pre.get(FundConf.ajdm_cbj.getIndex())).setScale(4, BigDecimal.ROUND_HALF_UP) + ");B基金(合并数量:" + Math.abs((double)row.get(FundConf.bjdm_yssl.getIndex())) + ";成本价:" + new BigDecimal((double)pre.get(FundConf.bjdm_cbj.getIndex())).setScale(4, BigDecimal.ROUND_HALF_UP) + ")");
                            }catch (Exception e){
                                row.set(FundConf.remark.getIndex(), "计算错误");
                            }
                            row.set(FundConf.mjdm_cjjg.getIndex(), new BigDecimal((double)row.get(FundConf.cccb_zy.getIndex()) / Math.abs((double)row.get(FundConf.mjdm_yssl.getIndex()))).setScale(4,BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.ajdm_cjjg.getIndex(), new BigDecimal((double)pre.get(FundConf.ajdm_cbj.getIndex())).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue());
                            row.set(FundConf.bjdm_cjjg.getIndex(), new BigDecimal((double)pre.get(FundConf.bjdm_cbj.getIndex())).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue());
                        }
                    }
                    pre = row;
                }
            }
            }catch (Exception e){
                row.set(FundConf.remark.getIndex(), e.getClass().getName());
                e.printStackTrace();
            }
        }
    }
    private List<Object> getFirstRow(String gdh, String jys, String zqdm, int seqno_qs, String jylb_qs, List<List<Object>> deliveries){
        for (List<Object> row: deliveries){
            if (row.get(FundConf.zqdm_fjmj.getIndex()).toString().equals(zqdm) &&
                    row.get(FundConf.gdh.getIndex()).toString().equals(gdh) &&
                    row.get(FundConf.jys.getIndex()).toString().equals(jys) &&
                    Integer.valueOf(row.get(FundConf.seqno_qs.getIndex()).toString()).intValue() == seqno_qs &&
                    row.get(FundConf.jylb_qs.getIndex()).toString().equals(jylb_qs)){
                return row;
            }
        }
        return null;
    }
    private void sort(List<List<Object>> deliveries){
        /**
         * 排序
         */
        Collections.sort(deliveries,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        String d1Str = d1.get(FundConf.gdh.getIndex()).toString() + d1.get(FundConf.jys.getIndex()) + d1.get(FundConf.zqdm_fjmj.getIndex()) + d1.get(FundConf.seqno_qs.getIndex()).toString();
                        String d2Str = d2.get(FundConf.gdh.getIndex()).toString() + d2.get(FundConf.jys.getIndex()) + d2.get(FundConf.zqdm_fjmj.getIndex()) + d2.get(FundConf.seqno_qs.getIndex()).toString();
                        int flag = d1Str.compareToIgnoreCase(d2Str);
                        if (flag == 0){
                            return Long.valueOf(d1.get(FundConf.seqno_qs.getIndex()).toString()).compareTo(Long.valueOf(d2.get(FundConf.seqno_qs.getIndex()).toString()));
                        }else{
                            return flag;
                        }
                    }
                });
    }
    private List<Object> tranDataType(List<String> delivery){
        /**
         * 字段格式标准化
         */
        List<Object> result = new ArrayList<>();
        for (int i=0; i<delivery.size(); i++){
            result.add(UdfUtil.parseObject(delivery.get(i), FundConf.values()[i].getType()));
        }
        return result;
    }
    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (FundConf conf: FundConf.values()){
            fieldNames.add(conf.toString());
            outOIS.add(UdfUtil.getInspector(conf.getType()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, outOIS);
    }
}

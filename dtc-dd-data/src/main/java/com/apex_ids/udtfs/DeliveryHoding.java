package com.apex_ids.udtfs;

import com.apex_ids.util.MyEnum.CostConf;
import com.apex_ids.util.UdfUtil;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
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

import java.math.BigDecimal;
import java.util.*;

@Description(
        name = "DeliveryHoding",
        extended = "交割流水成本迭代"
)
public class DeliveryHoding extends GenericUDTF {
    private static final Logger log = LoggerFactory.getLogger(DeliveryHoding.class);
    private static String JYLB_ZZ="11";
    static Set<String> jylbSp = Sets.newHashSet("65", "66", "7", "10", "31", "9", "21", "20", "15", "12");
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
            List<String> dl = (List<String>) rows.get(i);//转换
            deliveries.add(tranDataType(dl));
        }
        sort(deliveries);//排序
        List< List<List<Object>>> list=splitDelivery(deliveries);
        cost(list.get(0),null);//计算 普通品种
        cost(list.get(1),list.get(0));//债转股
        cost(list.get(2),list.get(2));//证券代码变更  20200513
        for (List<Object> delivery: deliveries) {
            forward(delivery.toArray());//输出
        }
    }

    @Override
    public void close() throws HiveException {

    }
    /**
     * 交割流水分类
     * 目前分为两类：普通品种、债转股
     * @param deliveryRows  交割流水数据集
     * @return
     */
    private static List<List<List<Object>>> splitDelivery(List<List<Object>> deliveryRows){
        Set<StockCode> jjbhcfStockCode = new HashSet<>(); //合并拆分的证券代码
        Set<StockCode> convertDebtCode = new HashSet<>(); //可转债股票
        Set<StockCode> codeChange = new HashSet<>(); //可转债股票
        List<List<List<Object>>> dels = new ArrayList<>();
        List<List<Object>> commonDels = new ArrayList<>();//普通品种
        List<List<Object>> convertDebtDels = new ArrayList<>();//债转股
        List<List<Object>> codeChangeDels = new ArrayList<>();// 证券代码变更 20200513
        // 当前客户存在合并拆分的证券代码保存起来
        for(List<Object> del : deliveryRows){
//            if(Constants.jylbHBCF.contains(del.get(CostConf.jylb.getIndex())+"")){//分级基金合并拆分
//                jjbhcfStockCode.add(new StockCode(del.get(CostConf.zqdm.getIndex())+"", del.get(CostConf.jys.getIndex())+"", del.get(CostConf.gdh.getIndex())+""));
//            }
            if (JYLB_ZZ.equals(del.get(CostConf.jylb.getIndex())+"") && (del.get(CostConf.zqlb.getIndex())+"").
                    endsWith("0") && new BigDecimal(del.get(CostConf.yssl.getIndex())+"").doubleValue() > 0){//债转股的股票
                //  convertDebtCode.add(new StockCode(del.getZqdm(), del.getJys(), del.getGdh()));
                convertDebtCode.add(new StockCode(del.get(CostConf.zqdm.getIndex())+"", del.get(CostConf.jys.getIndex())+"", del.get(CostConf.gdh.getIndex())+""));
            }
            if ((del.get(CostConf.cjbh.getIndex())+"").endsWith("内部代码变更")){//证券代码变更后的股票
            	codeChange.add(new StockCode(del.get(CostConf.zqdm.getIndex())+"", del.get(CostConf.jys.getIndex())+"", del.get(CostConf.gdh.getIndex())+""));
            }
        }
        for (List<Object> del: deliveryRows){
            StockCode stockCode = new StockCode(del.get(CostConf.zqdm.getIndex())+"", del.get(CostConf.jys.getIndex())+"", del.get(CostConf.gdh.getIndex())+"");
            if (/*!jjbhcfStockCode.contains(stockCode)
                    &&*/ !convertDebtCode.contains(stockCode) && !codeChange.contains(stockCode)){
                commonDels.add(del);
            }
            if (convertDebtCode.contains(stockCode)){
                convertDebtDels.add(del);
            }
            if (codeChange.contains(stockCode)){
                codeChangeDels.add(del);
            }
        }
        dels.add(commonDels);
        dels.add(convertDebtDels);
        dels.add(codeChangeDels);
        return dels;
    }
    private void cost(List<List<Object>> deliveries, Object object){
        /**
         * 交割迭代计算
         */
        String cust_sec = "";
        int v_new = 0;
        // 上条记录持仓成本、累计盈亏、证券数量、成本价
        double cccb_pre = 0, ljyk_pre = 0, zqsl_pre = 0, cbj_pre = 0;
        // 当前数据证券数量、持仓成本、成本价、实现盈亏、累计盈亏、交易费用、成交价格、应收数量
        double zqsl_cur = 0, cccb_cur = 0, cbj_cur = 0, sxyk_cur = 0, ljyk_cur = 0, jyfy_cur = 0, cjjg_cur = 0,  bczqye = 0;
        double ysje, cjsl, yssl;
        String jylb_qs, vs_zy, jylb, zqlb, cjbh; // 清算交易类别、摘要
        for (List<Object> row: deliveries) {
           try {
                String cust_sec_cur = row.get(CostConf.khh.getIndex()).toString()
                        + row.get(CostConf.gdh.getIndex())
                        + row.get(CostConf.jys.getIndex())
                        + row.get(CostConf.zqdm.getIndex());
                if (cust_sec.equals(cust_sec_cur)) {
                    v_new = 0;
                } else {
                    v_new = 1;
                }
                if (v_new == 1) {
                    cust_sec = cust_sec_cur;
                    if (!StringUtils.isBlank(row.get(CostConf.zqsl_cc.getIndex()).toString())) {//上日持仓
                        zqsl_pre = Double.valueOf(row.get(CostConf.zqsl_cc.getIndex()).toString());
                        cccb_pre = new BigDecimal(row.get(CostConf.cccb_cc.getIndex()).toString()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                        ljyk_pre = new BigDecimal(row.get(CostConf.ljyk_cc.getIndex()).toString()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                        try {
                            cbj_pre = zqsl_pre == 0 ? 0 : new BigDecimal(cccb_pre / zqsl_pre + "").setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    } else {//上日无持仓
                        zqsl_pre = 0;
                        cccb_pre = 0;
                        ljyk_pre = 0;
                        cbj_pre = 0;
                    }
                }
                jylb = row.get(CostConf.jylb.getIndex()).toString();
                zqlb = row.get(CostConf.zqlb.getIndex()).toString();
                cjbh = row.get(CostConf.cjbh.getIndex()).toString();
                cjjg_cur = Double.valueOf(row.get(CostConf.cjjg.getIndex()).toString());
                bczqye = Double.valueOf(row.get(CostConf.bczqye.getIndex()).toString());
                ysje = new BigDecimal(row.get(CostConf.ysje.getIndex()).toString()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                cjsl = new BigDecimal(row.get(CostConf.cjsl.getIndex()).toString()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                yssl = new BigDecimal(row.get(CostConf.yssl.getIndex()).toString()).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                jyfy_cur = Double.valueOf(row.get(CostConf.s1.getIndex()).toString()) +
                        Double.valueOf(row.get(CostConf.s2.getIndex()).toString()) +
                        Double.valueOf(row.get(CostConf.s3.getIndex()).toString()) +
                        Double.valueOf(row.get(CostConf.s4.getIndex()).toString()) +
                        Double.valueOf(row.get(CostConf.s5.getIndex()).toString()) +
                        Double.valueOf(row.get(CostConf.s6.getIndex()).toString());
                if (jylb.equals(Constants.JYLB_HSGK)) {
                    zqsl_cur = bczqye;
                } else if (Constants.jylbSGSH.contains(jylb) &&
                        Constants.cjbhList.contains(cjbh) &&
                        !Constants.ZQLB_HBETF.equals(zqlb)) {
                    zqsl_cur = zqsl_pre;
                } else {
                    zqsl_cur = zqsl_pre + Double.valueOf(row.get(CostConf.yssl.getIndex()).toString());
                    if(Constants.JYLB_CBTZ.contains(row.get(CostConf.jylb.getIndex())+"")){
                        cbj_pre = cbj_pre;
                        cccb_pre = zqsl_pre * cbj_pre;
                    }
                }
                if (zqsl_cur < 0 && bczqye >= 0) {
                    zqsl_cur = bczqye;
                } else if (zqsl_cur < 0 && bczqye < 0 && !(Constants.ZQLB_LOF.equals(zqlb) && Constants.jylbTGZRZC.contains(jylb) && Constants.CJBH_ZSBG.equals(cjbh))) {
                    zqsl_cur = 0;
                }
                row.set(CostConf.bczqye_qs.getIndex(),zqsl_cur);
                // 修正每一笔交易的成本、盈亏、摘要等信息
                // 对交易类别进行分类处理到VS_JYLB_QS变量中
                if (/*StringUtils.isBlank(row.get(CostConf.fj_khh.getIndex()) + "")*/ row.get(CostConf.fj_khh.getIndex()) == null) {
                    // 发生买入交易行为（1买入,16送股,61融资买入,41认购,42申购,59大宗买入,60大宗卖出,80申保,83中签）
                    if (Constants.jylbMR.contains(jylb) || (Constants.jylbSG.contains(jylb) && !Constants.CJBH_SGFQWT.equals(cjbh)) || (Constants.JYLB_JK.equals(jylb) && zqlb.startsWith("Z"))) {
                        if((row.get(CostConf.cjbh.getIndex())+"").endsWith("内部代码变更")) {
                        	jylb_qs = jylb;
                            jyfy_cur = 0;
                        } else {
                        	jylb_qs = Constants.JYLB_BUY;
                        }
                        // 发生卖出交易行为（2卖出,6分红,17兑付,22兑息,43赎回,45分红,71融资强平,81退保）
                    } else if (Constants.jylbSY.contains(jylb) || (Constants.JYLB_JJSH.equals(jylb) && !Constants.cjbhList.contains(cjbh))) {
                        jylb_qs = Constants.JYLB_SELL;
                    } else {
                        jylb_qs = jylb;
                        jyfy_cur = 0;
                    }
                    vs_zy = "";
                    //更新持仓成本、累计盈亏等
                    if (jylb_qs.equals(Constants.JYLB_BUY) && ysje != 0) {
                        cccb_cur = cccb_pre + Math.abs(ysje);
                        cbj_cur = zqsl_cur == 0 ? 0 : new BigDecimal(cccb_cur / zqsl_cur+"").setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                        sxyk_cur = 0;
                        ljyk_cur = ljyk_pre;
                        vs_zy = "本次交易后持股:" + zqsl_cur + ";成交价:" + cjjg_cur + ";买入前成本价:" + cbj_pre + ";买入后成本价:" + cbj_cur;
                    } else if (jylb_qs.equals(Constants.JYLB_SELL) && ysje != 0) {
                        cbj_cur = cbj_pre;
                        if (jylb.equals(Constants.JYLB_HSGK) && zqlb.equals(Constants.ZQLB_LOF)) {
                            cccb_cur = cccb_pre;
                            sxyk_cur = 0;
                            ljyk_cur = 0;
                        } else {
                            cccb_cur = new BigDecimal(cbj_cur * zqsl_cur+"").setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            if (Constants.JYS_SH.equals(row.get(CostConf.jys.getIndex())) && Constants.ZQLB_LOF.equals(zqlb) && Constants.CJBH_JSSH.equals(cjbh) && yssl == 0 && cjsl != 0) {
                                yssl = 0 - cjsl;
                            }else{
                                yssl=new BigDecimal(row.get(CostConf.yssl.getIndex())+"").doubleValue();
                            }
                            sxyk_cur = new BigDecimal(Math.abs(ysje) - Math.abs(yssl) * cbj_cur+"").setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            ljyk_cur = new BigDecimal(ljyk_pre + sxyk_cur+"").setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                        }
                        vs_zy = "本次交易后持股:" + zqsl_cur + ";成交价:" + cjjg_cur + ";成本价:" + cbj_cur + ";盈利:" + sxyk_cur;
                    } else if(JYLB_ZZ.equals(jylb_qs)){//add by caigx
                        if (yssl < 0 && (row.get(CostConf.zqlb.getIndex())+"").startsWith("Z")){//按卖出算
                            cbj_cur=cbj_pre;
                            row.set(CostConf.ysje.getIndex(),new BigDecimal(cbj_cur*yssl).abs().setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
                            cccb_cur = new BigDecimal(cbj_cur * zqsl_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            sxyk_cur = 0;
                            ljyk_cur = 0;
                            vs_zy = "本次债转股交易债券转出股份:" + yssl + ";转移出成本:" + row.get(CostConf.ysje.getIndex());
                        }
                        if (yssl > 0 && (row.get(CostConf.zqlb.getIndex())+"").endsWith("0")){//按买入算
                            double ysje_zg = 0;
                            try{
                                String zqdm = row.get(CostConf.cjbh.getIndex()).toString().substring(0, 6);
                                ysje_zg = getConvertDebt((List<List<Object>>) object, zqdm);
                            }catch (Exception e){
                                System.out.println("转入股票未取到债券信息");
                            }
                            if (ysje_zg == 0d && cjjg_cur != 0d){
                                ysje_zg = cjjg_cur * Math.abs(yssl);
                            }

//                            String zqdm = (row.get(CostConf.cjbh.getIndex())+"").substring(0, 6);
//                            ysje_zg = new BigDecimal(getConvertDebt((List<List<Object>>) object, zqdm));
                            cccb_cur = cccb_pre + Math.abs(ysje_zg);
                            row.set(CostConf.ysje.getIndex(), 0-ysje_zg);
                            cbj_cur = zqsl_cur == 0 ? 0 :new BigDecimal(cccb_cur / zqsl_cur).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                            sxyk_cur = 0;
                            ljyk_cur = new BigDecimal(ljyk_pre + sxyk_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            vs_zy = "本次债转股交易股票转入股份:" + yssl + ";转移到成本:" + ysje_zg;
                        }
                    }else if((row.get(CostConf.cjbh.getIndex())+"").endsWith("内部代码变更")){//add by QiuJianFeng
                    	if (yssl < 0 ){// 19,托管转出,变更前证券代码交割记录，按比例修正应收金额为上一笔交易持仓成本（变更当日可交易，故不能直接取上日成本）
                        	cbj_cur=cbj_pre;
                            row.set(CostConf.ysje.getIndex(),new BigDecimal(Math.abs(cbj_cur*yssl)).setScale(2,BigDecimal.ROUND_HALF_UP).doubleValue());
                            cccb_cur = new BigDecimal(cbj_cur * zqsl_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            sxyk_cur = 0;
                            ljyk_cur = 0;
                            vs_zy = "本次证券代码变更转出股份:" + yssl + ";成本:" + row.get(CostConf.ysje.getIndex());
                        }
                    	if (yssl > 0 ){// 18，托管转入， 变更后证券代码交割记录，按比例修正应收金额为变更前证券代码的上日持仓成本
                    		double ysje_bg = 0;
                            try{
                                String zqdm = row.get(CostConf.cjbh.getIndex()).toString().substring(0, 6);
                                ysje_bg = getStockChange((List<List<Object>>) object, zqdm);  // 此处已经将应收金额转为负数
                            }catch (Exception e){
                                System.out.println("变更后股票未取到原股票信息");
                            }
                            row.set(CostConf.ysje.getIndex(), ysje_bg);
                            sxyk_cur = 0;
                            ljyk_cur = new BigDecimal(ljyk_pre + sxyk_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            vs_zy = "本次证券代码变更后股票转入股份:" + yssl + ";成本:" + ysje_bg;
                        }
                    }else if (jylbSp.contains(jylb_qs) && new BigDecimal(row.get(CostConf.cjje.getIndex())+"").doubleValue() != 0) {
                        if (new BigDecimal(row.get(CostConf.yssl.getIndex()) + "").doubleValue() > 0) {
                            cccb_cur = cccb_pre + new BigDecimal(row.get(CostConf.cjje.getIndex()) + "").abs().doubleValue();
                            cbj_cur = zqsl_cur == 0 ? 0 : new BigDecimal(cccb_cur / zqsl_cur).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                            sxyk_cur = 0;
                            ljyk_cur = ljyk_pre;
                            vs_zy = "本次交易后持股:" + zqsl_cur + ";成交价:" + cjjg_cur + ";买入前成本价:" + cbj_pre + ";买入后成本价:"
                                    + cbj_cur;
                        }
                        if (new BigDecimal(row.get(CostConf.yssl.getIndex())+"").doubleValue() < 0) {
                            cbj_cur = cbj_pre;
                            cccb_cur = new BigDecimal(cbj_cur * zqsl_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            sxyk_cur = new BigDecimal(new BigDecimal(row.get(CostConf.cjje.getIndex())+"").abs().doubleValue() - Math.abs(yssl) * cbj_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            ljyk_cur = new BigDecimal(ljyk_pre + sxyk_cur).setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
                            vs_zy = "本次交易后持股:" + zqsl_cur + ";成交价:" + cjjg_cur + ";成本价:" + cbj_cur + ";盈利:"
                                    + sxyk_cur;
                        }
                    }else {
                        cbj_cur = cbj_pre;
                        cccb_cur = cccb_pre;
                        sxyk_cur = 0;
                        ljyk_cur = ljyk_pre;
                        if (Constants.JYLB_JJSH.equals(jylb)
                                && Constants.CJBH_SHFQWT.equals(cjbh)) {
                            vs_zy = "发起赎回委托";
                        } else if (Constants.JYLB_JJSH.equals(jylb)
                                && Constants.CJBH_SHJSCG.equals(cjbh)) {
                            vs_zy = "赎回到期交收";
                        }
                    }
                    row.set(CostConf.bczqye_qs.getIndex(), zqsl_cur);
                    row.set(CostConf.cccb_qs.getIndex(), cccb_cur);
                    row.set(CostConf.cbj_qs.getIndex(), Double.parseDouble(new BigDecimal(cbj_cur).setScale(4, BigDecimal.ROUND_HALF_UP)+""));
                    row.set(CostConf.sxyk_qs.getIndex(), sxyk_cur);
                    row.set(CostConf.ljyk_qs.getIndex(), ljyk_cur);
                    row.set(CostConf.remark_qs.getIndex(), vs_zy);
                    row.set(CostConf.zt_qs.getIndex(), 1);
                    row.set(CostConf.jyfy.getIndex(), jyfy_cur);
                    zqsl_pre = zqsl_cur;
                    cccb_pre = cccb_cur;
                    ljyk_pre = ljyk_cur;
                    cbj_pre = cbj_cur;
                }
            } catch (Exception e) {
                row.set(CostConf.remark_qs.getIndex(), e.toString());
                e.printStackTrace();
            }
        }
    }
    /**
     * 获取股票对应债券转股时发生的升本
     * @param deliveryRows  普通品种交割
     * @param zqdm          债券代码
     * @return
     */
    private static Double getConvertDebt(List<List<Object>> deliveryRows, String zqdm){
        for (List<Object> del: deliveryRows){
            if ((del.get(CostConf.jylb.getIndex())+"").equals(JYLB_ZZ) && new BigDecimal(del.get(CostConf.ysje.getIndex())+"").doubleValue() > 0
                    && new BigDecimal(del.get(CostConf.yssl.getIndex())+"").doubleValue() < 0 && (del.get(CostConf.zqlb.getIndex())+"").startsWith("Z")
                    && (del.get(CostConf.zqdm.getIndex())+"").equals(zqdm)){
                return new BigDecimal(del.get(CostConf.ysje.getIndex())+"").doubleValue();
            }
        }
        return 0D;
    }
    private static Double getStockChange(List<List<Object>> deliveryRows, String zqdm){
        for (List<Object> del: deliveryRows){
            if (((del.get(CostConf.cjbh.getIndex())+"").endsWith("内部代码变更") && (del.get(CostConf.zqdm.getIndex())+"").equals(zqdm) && new BigDecimal(del.get(CostConf.yssl.getIndex())+"").doubleValue() < 0)){
                return new BigDecimal(del.get(CostConf.ysje.getIndex())+"").doubleValue() * -1;
            }
        }
        return 0D;
    }
    private void sort(List<List<Object>> deliveries){
        /**
         * 排序
         */
        Collections.sort(deliveries,
                new Comparator<List<Object>>() {
                    @Override
                    public int compare(List<Object> d1, List<Object> d2) {
                        try{
                            String d1Str = d1.get(CostConf.gdh.getIndex()).toString() + d1.get(CostConf.jys.getIndex()) + d1.get(CostConf.zqdm.getIndex());
                            String d2Str = d2.get(CostConf.gdh.getIndex()).toString() + d2.get(CostConf.jys.getIndex()) + d2.get(CostConf.zqdm.getIndex());
                            int flag = d1Str.compareToIgnoreCase(d2Str);
                            if (flag == 0){
                                return Long.valueOf(d1.get(CostConf.seqno_qs.getIndex()).toString()).compareTo(Long.valueOf(d2.get(CostConf.seqno_qs.getIndex()).toString()));
                            }else{
                                return flag;
                            }
                        }catch (Exception e){

                        }
                        return 1;
                    }
                });
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
    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */
        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (CostConf conf: CostConf.values()){
            fieldNames.add(conf.toString());
            outOIS.add(UdfUtil.getInspector(conf.getType()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, outOIS);
    }


}
class StockCode {

    //	private static final long serialVersionUID = -7953156148793344658L;
    private String zqdm;
    private String jys;
    private String gdh;

    public StockCode() {
    }

    public StockCode(String zqdm, String jys, String gdh) {
        this.zqdm = zqdm;
        this.jys = jys;
        this.gdh = gdh;
    }

    public String getZqdm() {
        return zqdm;
    }

    public void setZqdm(String zqdm) {
        this.zqdm = zqdm;
    }

    public String getJys() {
        return jys;
    }

    public void setJys(String jys) {
        this.jys = jys;
    }

    public String getGdh() {
        return gdh;
    }

    public void setGdh(String gdh) {
        this.gdh = gdh;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((zqdm == null) ? 0 : zqdm.hashCode());
        result = prime * result + ((jys == null) ? 0 : jys.hashCode());
        result = prime * result + ((gdh == null) ? 0 : gdh.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        StockCode other = (StockCode) obj;
        if (zqdm == null) {
            if (other.zqdm != null)
                return false;
        } else if (!zqdm.equals(other.zqdm))
            return false;
        if (jys == null) {
            if (other.jys != null)
                return false;
        } else if (!jys.equals(other.jys))
            return false;
        if (gdh == null) {
            if (other.gdh != null)
                return false;
        } else if (!gdh.equals(other.gdh))
            return false;
        return true;
    }

    public Boolean isEmpty(){
        if(zqdm == null && jys == null){
            return true;
        }
        else{
            return false;
        }
    }
    //@Override
    public String toString() {
        if (jys != null)
            return zqdm + "." + jys;
        return zqdm;
    }

}

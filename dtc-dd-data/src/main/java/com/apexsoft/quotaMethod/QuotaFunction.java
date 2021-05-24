package com.apexsoft.quotaMethod;


import com.apexsoft.utils.DateUtils;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class QuotaFunction {
    public static Double toplimit = 2.9;
    public static Double demandIncome = null;
    /**
     * 根据一段时间内的资产及转入转出，算收益率
     * 计算区间收益率
     * C0、C1、C2、C3...Cn分别代表为期初到n期的现金流量，负值代表现金流入，正值代表现金流出。
     * 0 = C0 + C1/(1+rate)^1 + C2/(1+rate)^2 + C3/(1+rate)^3....+ Cn/(1+rate)^n
     * C0 = 初期总资产， Cn = 期末总资产
     * @return
     */
    public static double calcSyl(double[] days, double[] zrzcs, int beginDate, int endDate){
        double syl = 0.1;
        double range = Math.abs(syl);
        double errorRange = 0.000005;
        double shift, lastShift = 0.0;//净现值
        boolean isFind = false;
        days[0] = (double) beginDate;
        int size = days.length;
        days[days.length-1] = (double) endDate;
        //考虑到初期资产为0的情况,会导致计算值异常
        double[] zrzcArray = new double[zrzcs[0]==0.0 ? size-1: size];
        double[] dayArray = new double[zrzcs[0]==0.0 ? size-1: size];
        if (zrzcs[0] == 0.0){
            System.arraycopy(zrzcs, 1, zrzcArray, 0, size-1);
            System.arraycopy(days, 1, dayArray, 0, size-1);
            zrzcArray[0] = Math.abs(zrzcArray[0]);
        }else{
            zrzcArray = zrzcs;
            dayArray = days;
        }
        if (zrzcArray.length > 2){
            double startValue = 0.0 - zrzcArray[0];//第一天转入，设为负
            double daysCount = (double)differentDays((int)dayArray[0], (int)dayArray[dayArray.length-1], true);
            //System.out.println("自然日：" + daysCount);
            for (int nStep=1; nStep<=1000; nStep++){
                shift = startValue;
                for (int i=1; i<=zrzcArray.length-1; i++){
                    double day = differentDays((int)dayArray[0], (int)dayArray[i], true);
                    shift = shift + zrzcArray[i]/ Math.pow(1 + syl, day/daysCount);
//                    shift_i = shift_i + zrzcArray[i]/ Math.pow(1 + syl, 1);
                }
                if (shift > errorRange){
                    if (lastShift < -errorRange){
                        isFind = true;
                    }
                    if (!isFind){
                        range *= 2.0;
                    }else{
                        range /= 2.0;
                    }
                    lastShift = shift;
                    syl += range;
                }else if (shift < -errorRange){
                    if (lastShift > errorRange){
                        isFind = true;
                    }
                    if (!isFind) {
                        range *= 2.0;
                    }else{
                        range /= 2.0;
                    }
                    lastShift = shift;
                    double temp = syl - range;
                    if (temp < -1){//回报率不允许小于-1(不会出现亏损多于本金情况)
                        range -= (-1 - temp);
                        range /= 2.0;
                    }
                    syl -= range;
                }else{
                    break;
                }
                //System.out.println(String.format("计算次数：%d, 当前回报率：%.8f, range=%.8f, shift=%.8f, lastShift=%.8f", calcCount, syl, range, shift, lastShift));
            }
            return syl;
        }else if (zrzcArray.length == 2){
            return calcZzl(zrzcArray[0], zrzcArray[1]);
        }else{
            return 0.0;
        }
    }
    public static double calcSylIrr(double[] zrzcs){
        double syl = 0.1;
        double range = Math.abs(syl);
        double errorRange = 0.000005;
        double shift, lastShift = 0.0;//净现值
        boolean isFind = false;
        int size = zrzcs.length;
        //考虑到初期资产为0的情况,会导致计算值异常
        double[] zrzcArray = new double[zrzcs[0]==0.0 ? size-1: size];
        if (zrzcs[0] == 0.0){
            System.arraycopy(zrzcs, 1, zrzcArray, 0, size-1);
            zrzcArray[0] = Math.abs(zrzcArray[0]);
        }else{
            zrzcArray = zrzcs;
        }
        if (zrzcArray.length > 2){
            double startValue = 0.0 - zrzcArray[0];//第一天转入，设为负
            for (int nStep=1; nStep<=1000; nStep++){
                shift = startValue;
                for (int i=1; i<=zrzcArray.length-1; i++){
                    shift = shift + zrzcArray[i]/ Math.pow(1 + syl, 1);
                }
                if (shift > errorRange){
                    if (lastShift < -errorRange){
                        isFind = true;
                    }
                    if (!isFind){
                        range *= 2.0;
                    }else{
                        range /= 2.0;
                    }
                    lastShift = shift;
                    syl += range;
                }else if (shift < -errorRange){
                    if (lastShift > errorRange){
                        isFind = true;
                    }
                    if (!isFind) {
                        range *= 2.0;
                    }else{
                        range /= 2.0;
                    }
                    lastShift = shift;
                    double temp = syl - range;
                    if (temp < -1){//回报率不允许小于-1(不会出现亏损多于本金情况)
                        range -= (-1 - temp);
                        range /= 2.0;
                    }
                    syl -= range;
                }else{
                    break;
                }
            }
            return syl;
        }else if (zrzcArray.length == 2){
            return calcZzl(zrzcArray[0], zrzcArray[1]);
        }else{
            return 0.0;
        }
    }
    private static double calcZzl(double qczc, double qmzc){
        try {
            double syl = qczc == 0.0 ? 0.0 : (qmzc-qczc)/qczc;
            return syl;
        }catch (Exception e){
            e.printStackTrace();
            return 0.0;
        }
    }
    private static int differentDays(int date1, int date2, Boolean addLastDay){
        String str1 = String.valueOf(date1);
        String str2 = String.valueOf(date2);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        try{
            Date date11 = format.parse(str1);
            Date date22 = format.parse(str2);
            int temp = (int) ((date22.getTime() - date11.getTime()) / (1000*3600*24));
            if (addLastDay){
                return (temp + 1);
            }else {
                return temp;
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public static double calcZdhcl(List<Double> jzList){
        double zdhcl = 0.0;
        double hcl = 0.0;
        double zxjz = 0.0;
        for (int i=0; i<jzList.size()-1; i++){
            zxjz = jzList.get(i);
            if (zxjz != 0.0){
                hcl = new BigDecimal((zxjz - Collections.min(jzList.subList(i+1, jzList.size()))) / zxjz).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                zdhcl = hcl > zdhcl ? hcl: zdhcl;
            }
        }
        return zdhcl;
    }
    public static String calcZdhchfts(List<Integer> rqList, List<Double> jzList){
        String describe = "";
        int ts = 0;
        double zdhcl = 0.0;
        double hcl = 0.0;
        double zxjz = 0.0;
        int hcks = 0;
        int hcjs = 0;
        for (int i=0; i<jzList.size()-1; i++){
            zxjz = jzList.get(i);
            if (zxjz != 0.0){
                List<Double> subList = jzList.subList(i+1, jzList.size());
                Double min = Collections.min(subList);
                hcl = new BigDecimal((zxjz - min) / zxjz).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
                if (hcl > zdhcl){
                    hcks = i;
                    int index = subList.indexOf(min);
                    hcjs = i + index + 1;
                    zdhcl = hcl;
                }
            }
        }
        List<Double> hchfList = jzList.subList(hcjs+1, jzList.size());
        double hccsjz = jzList.get(hcks);
        int hchf = -1;
        Double max = 0.0;
        for (int i=0; i<hchfList.size(); i++){
            if (hchfList.get(i) > max){
                max = hchfList.get(i);
            }
            if (hchfList.get(i) >= hccsjz){
                hchf = i + hcjs + 1;
                break;
            }
        }
        if (hchf > 0){
            describe = String.format("完整回撤恢复时长：%d交易日,起始：%d,恢复完成：%d", hchf - hcjs, rqList.get(hcjs + 1), rqList.get(hchf));
        }else if (hcjs == rqList.size()-1){
            describe = String.format("回撤结束为最后一天，并没有进行恢复");
        }else {
            int index = hchfList.indexOf(max) + hcjs + 1;
            try {
                String hcbfb = new BigDecimal((jzList.get(index)-jzList.get(hcjs))/(jzList.get(hcks)-jzList.get(hcjs))*100.0).setScale(2, BigDecimal.ROUND_HALF_UP).toString();
                describe = String.format("完成%s回撤恢复时长：%d交易日,起始：%d,恢复截至：%d",hcbfb + "%", index - hcjs, rqList.get(hcjs + 1), rqList.get(index));
            }catch (Exception e){
                describe = "计算错误";
            }

        }
//        ts = hchf - hcjs;
        return describe;
    }

    public static double[] tranListToArray(List<Double> data){
        double[] tmp = new double[data.size()];
        for (int i=0; i<data.size(); i++){
            tmp[i] = data.get(i);
        }
        return tmp;
    }

    /**
     * 获取盈亏率
     * @param data 日账单数据
     *             ZZC    ZFZ    ZCJLR    DRYK    CRJE    ZRZQSZ    QTSR    QCJE    ZCZQSZ    QTZC
     *             总资产  总负债  资产净流入 当日盈亏 存入金额 转入市值    其他收入 取出金额 转出市值    其他支出
     * @return 收益率
     */
    public static double getYkl(List<Map<String, Object>> data){
        int count = data.size(); // 数据天数
        double zr = 0d; //转入值
        double zc = 0d; //转出值
        //期初资产
        double qczc = Double.valueOf(data.get(0).get("ZZC").toString())
                - Double.valueOf(data.get(0).get("ZFZ").toString())
                - Double.valueOf(data.get(0).get("ZCJLR").toString())
                - Double.valueOf(data.get(0).get("DRYK").toString());
        for (int i=0; i<data.size(); i++){
            zr += Double.valueOf(data.get(i).get("CRJE").toString())//存入金额
                    + Double.valueOf(data.get(i).get("ZRZQSZ").toString())//转入证券市值
                    + Double.valueOf(data.get(i).get("QTSR").toString());///其他收入
            zc += Double.valueOf(data.get(i).get("QCJE").toString())//取出金额
                    + Double.valueOf(data.get(i).get("ZCZQSZ").toString())//转出证券市值
                    + Double.valueOf(data.get(i).get("QTZC").toString());//其他支出
        }
        //期末资产
        double qmzc = Double.valueOf(data.get(count-1).get("ZZC").toString())
                - Double.valueOf(data.get(count-1).get("ZFZ").toString());
        //总投资
        double ztz = qczc + zr;
        //总获得
        double zhd = qmzc + zc;
        //计算收益率
        return ztz == 0d ? 0d: zhd/ztz-1.0;
    }


    /**
     * 获取盈亏率
     * @param data 日账单数据
     *             ZZC    ZFZ    ZCJLR    DRYK    ZJYE      RQ
     *             总资产  总负债  资产净流入 当日盈亏 资金余额   日期
     * @return 收益率
     */
    public static double getSyl(List<Map<String, Object>> data){
        double qczc;
        double qmzc;
        int beginIndex = 0;
        int endIndex = data.size() - 1;
        for (int i=0; i<data.size(); i++){
            Map<String, Object> row = data.get(i);
            if (Double.valueOf(row.get("ZZC").toString()).doubleValue() != Double.valueOf(row.get("ZJYE").toString()).doubleValue()){
                beginIndex = i;
                break;
            }
        }
        for (int i=endIndex; i>=0; i--){
            Map<String, Object> row = data.get(i);
            if (Double.valueOf(row.get("ZZC").toString()).doubleValue() != Double.valueOf(row.get("ZJYE").toString()).doubleValue()){
                endIndex = i;
                break;
            }
        }
        if (beginIndex == endIndex){
            qczc = Double.valueOf(data.get(beginIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(beginIndex).get("ZFZ").toString())
                    - Double.valueOf(data.get(beginIndex).get("DRYK").toString());
            qmzc = Double.valueOf(data.get(endIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(endIndex).get("ZFZ").toString());
            return calcZzl(qczc, qmzc);
        }else {
            beginIndex -= 1;
            beginIndex = beginIndex < 0 ? 0: beginIndex;
            endIndex += 2;
            endIndex = endIndex > data.size() ? data.size(): endIndex;
        }
        List<Map<String, Object>> tradeDatas = data.subList(beginIndex, endIndex);
        List<Double> zrzcList = new ArrayList<>();
        List<Double> dayList = new ArrayList<>();
        qczc = Double.valueOf(tradeDatas.get(0).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZFZ").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZCJLR").toString())
                - Double.valueOf(tradeDatas.get(0).get("DRYK").toString());
        zrzcList.add(qczc);
        dayList.add(Double.valueOf(tradeDatas.get(0).get("RQ").toString()));
        for (int i=0; i<tradeDatas.size(); i++){
            double zcjlr = Double.valueOf(tradeDatas.get(i).get("ZCJLR").toString());
            if (zcjlr != 0.0){
                zrzcList.add(0.0 - zcjlr);
                dayList.add(Double.valueOf(tradeDatas.get(i).get("RQ").toString()));
            }
        }
        qmzc = Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("ZFZ").toString());
        zrzcList.add(qmzc);
        dayList.add(Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("RQ").toString()));
        double syl = calcSyl(tranListToArray(dayList),
                tranListToArray(zrzcList),
                (int) dayList.get(0).doubleValue(),
                (int) dayList.get(dayList.size()-1).doubleValue());
//        for (int i=0; i< dayList.size(); i++){
//            System.out.println(String.format("rq:%d   value:%.2f", (int)dayList.get(i).doubleValue(), zrzcList.get(i)));
//        }
        return syl;
    }

    /**
     * 收益率组合算法
     * 优先xirr
     * xirr超过上限计算累积成本
     * 累积成本超过上限则返回xirr
     * @param data
     * RQ    ZZC    ZFZ    ZJYE    DRYK    ZCJLR    QTSR    CRJE    QCJE    ZRZQSZ      ZCZQSZ
     * 日期   总资产  总负债  资金余额 当日盈亏 资产净流入 其它收入  存入金额 取出金额 转入市值     转出市值
     * @return
     */
    public static double getReturnRateByMultiFunc(List<Map<String, Object>> data){
        if (toplimit == null){
            try{
                Properties props = new Properties();
                props.load(QuotaFunction.class.getResourceAsStream("/config.properties"));
                toplimit = Double.valueOf(props.getProperty("ids.syl.toplimit"));
                demandIncome = Double.valueOf(props.getProperty("ids.syl.demandIncome"));
            }catch (Exception e){
                toplimit = 10.0;
            }
        }
        return getReturnRateByMultiFunc(data, toplimit);
    }

    /**
     *
     * @param data
     */
    public static void addGrowthRate(List<Map<String, Object>> data){
        List<Object> tradeinfo = getTradeDatas(data);
        if (tradeinfo.get(0) == null){//无交易数据
            for (Map<String, Object> map: data){
                map.put("gdsyl", 0d);
            }
            return;
        }
        int beginIndex = Integer.valueOf(tradeinfo.get(1)+"").intValue();
        int endIndex = Integer.valueOf(tradeinfo.get(2)+"").intValue();
        List<Map<String, Object>> tradeDatas = (List<Map<String, Object>>) tradeinfo.get(0);
        if (beginIndex == endIndex){//只有一笔交易数据
            double syl = 0d;
            if (interestSettleAccount(tradeDatas)){
                syl = getDemandIncome();
            }else{
                double qczc = Double.valueOf(data.get(beginIndex).get("ZZC").toString())
                        - Double.valueOf(data.get(beginIndex).get("ZFZ").toString())
                        - Double.valueOf(data.get(beginIndex).get("DRYK").toString());
                double qmzc = Double.valueOf(data.get(endIndex).get("ZZC").toString())
                        - Double.valueOf(data.get(endIndex).get("ZFZ").toString());
                syl = calcZzl(qczc, qmzc);
            }
            for (int i=0; i<data.size(); i++){
                if (i >= beginIndex){
                    data.get(i).put("gdsyl", new BigDecimal(syl).setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue());
                }else{
                    data.get(i).put("gdsyl", 0d);
                }
            }
            return;

        }

        Object[] funcInfo = getReturnRateAndFunc(tradeDatas, toplimit);
        String func = funcInfo[1]+"";
        System.out.println("use " + func);
        System.out.println(String.format("beginIndex:%d,endIndex:%d", beginIndex, endIndex));
        double syl = new BigDecimal(funcInfo[0]+"").setScale(6, BigDecimal.ROUND_HALF_UP).doubleValue();
        double gdyk = 0d;
        double gdsyl = 0d;
        for (int i=0; i<data.size(); i++){
            gdyk += Double.valueOf(data.get(i).get("DRYK")+"");
            data.get(i).put("gdyk", BigDecimal.valueOf(gdyk).setScale(2, BigDecimal.ROUND_HALF_UP));
            if (i < beginIndex){
                data.get(i).put("gdsyl", 0d);
            }else if (i <= endIndex){
                double drykl = calcYkl(data.get(i));
                gdsyl = (1.0+gdsyl)*(1.0+drykl)-1.0;
                data.get(i).put("gdsyl", gdsyl);
            }else {
                data.get(i).put("gdsyl", gdsyl);
            }
        }
        double days = differentDays(Double.valueOf(data.get(beginIndex).get("RQ")+"").intValue(), Double.valueOf(data.get(endIndex).get("RQ")+"").intValue(), true);
        double shift = syl - Double.valueOf(data.get(data.size()-1).get("gdsyl")+"");
        for (int i=0; i<data.size(); i++){
            if (i < endIndex){
                double day = differentDays(Double.valueOf(data.get(beginIndex).get("RQ")+"").intValue(), Double.valueOf(data.get(i).get("RQ")+"").intValue(), true);
                gdsyl = Double.valueOf(data.get(i).get("gdsyl")+"")+shift*day/days;
                data.get(i).put("gdsyl", gdsyl);
            }else if (i == endIndex){
                gdsyl = syl;
                data.get(i).put("gdsyl", gdsyl);
            }else {
                data.get(i).put("gdsyl", gdsyl);
            }
        }
    }

    private static double getReturnRate(List<Map<String, Object>> tradeDatas, String func){
        if (func.equals("xirr")){
            return calcXirr(tradeDatas);
        }else if (func.equals("ljcc")){
            return getSylByCumulativeCost(tradeDatas);
        }else if (func.equals("pjjzc")){
            return getSylByJzc(tradeDatas);
        }else if (func.equals("zdbje")){
            return getSylZdbje(tradeDatas);
        }else if (func.equals("lx")){
            if (interestSettleAccount(tradeDatas)){
                return getDemandIncome();
            }else {
                return 0d;
            }
        }
        else {
            double qczc = Double.valueOf(tradeDatas.get(0).get("ZZC").toString())
                    - Double.valueOf(tradeDatas.get(0).get("ZFZ").toString())
                    - Double.valueOf(tradeDatas.get(0).get("DRYK").toString());
            double qmzc = Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("ZZC").toString())
                    - Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("ZFZ").toString());
            return calcZzl(qczc, qmzc);
        }
    }
    private static double calcYkl(Map<String, Object> bill){
        try {
            double yk = Double.valueOf(bill.get("DRYK")+"");
            double zcjlr = Double.valueOf(bill.get("ZCJLR")+"");
            double zrzqsz = Double.valueOf(bill.get("ZRZQSZ")+"");
            double crje = Double.valueOf(bill.get("CRJE")+"");
            double zzc = Double.valueOf(bill.get("ZZC")+"");
            double zfz = Double.valueOf(bill.get("ZFZ")+"");
            double ykl = 0d;
            double preNetAssert = zzc - zfz - zcjlr - yk;
            if (preNetAssert == 0d){//上日资产为0
                if ((crje + zrzqsz) == 0d){
                    ykl = 0d;
                }else {
                    ykl = ((zzc-zfz)-(preNetAssert)-zcjlr)/(crje + zrzqsz);
                }
            }else if (isInterest(bill, preNetAssert)){//利息收入异常
                ykl = getDemandIncome();
            }else if (isQuickTrade(bill)){//快交
                ykl = yk/(preNetAssert+crje+zrzqsz);
            }else if (zcjlr < 0d){//资产净流入
                ykl = (zzc-zfz-zcjlr)/(preNetAssert)-1.0;
            }else if (zcjlr > 0d){//资产净流入
                if ((preNetAssert+zcjlr) == 0d){
                    ykl = 0d;
                }else {
                    ykl = (zzc-zfz)/(preNetAssert+zcjlr)-1.0;
                }
            }else {
                ykl = (zzc-zfz)/(preNetAssert)-1.0;
            }
            if (yk < 0d){
                ykl = 0d - Math.abs(ykl);
            }else {
                ykl = Math.abs(ykl);
            }
            return ykl;
        }catch (Exception e){
            e.printStackTrace();
            return 0d;
        }
    }
    private static boolean isQuickTrade(Map<String, Object> bill){
        boolean quickTrade = false;
        double zrzqsz = Double.valueOf(bill.get("ZRZQSZ")+"");
        double zczqsz = Double.valueOf(bill.get("ZCZQSZ")+"");
        double crje = Double.valueOf(bill.get("CRJE")+"");
        double qcje = Double.valueOf(bill.get("QCJE")+"");
        double zzc = Double.valueOf(bill.get("ZZC")+"");
        if (crje != 0d && qcje != 0d){
            double lr = crje + zrzqsz;
            double lc = qcje + zczqsz;
            double bl = lr / lc;
            if (bl >= 0.8 && bl <= 1.2){
                double tmp = Collections.max(Arrays.asList(lr, lc));
                if (zzc / tmp <= 0.4){
                    quickTrade = true;
                }
            }
        }
        return quickTrade;
    }
    private static boolean isInterest(Map<String, Object> bill, double preAssert){
        double rate = getDemandIncome();
        boolean interest = false;
        if (!bill.containsKey("QTSR")){
            return interest;
        }
        double qtsr = Double.valueOf(bill.get("QTSR")+"");
        if (qtsr == Double.valueOf(bill.get("DRYK")+"").doubleValue()
                && qtsr >= preAssert * rate
                && qtsr > 0d && qtsr >= Double.valueOf(bill.get("ZZC")+"").doubleValue() * rate){ //利息收入异常
            interest = true;
        }
        return interest;
    }
    public static Object[] getReturnRateAndFunc(List<Map<String, Object>> data, double limit){
        double qczc;
        double qmzc;
        Object[] objects = new Object[2];
        List<Object> tradeinfo = getTradeDatas(data);
        if (tradeinfo.get(0) == null){//无交易数据
            objects[0] = 0D;
            objects[1] = "none";
            return objects;
        }
        List<Map<String, Object>> tradeDatas = (List<Map<String, Object>>)tradeinfo.get(0);
        int beginIndex = Integer.valueOf(tradeinfo.get(1)+"").intValue();
        int endIndex = Integer.valueOf(tradeinfo.get(2)+"").intValue();
        if (interestSettleAccount(tradeDatas)){
            objects[0] = getDemandIncome();
            objects[1] = "lx";
            return objects;
        }
        if (beginIndex == endIndex){//只有一笔交易数据
            qczc = Double.valueOf(data.get(beginIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(beginIndex).get("ZFZ").toString())
                    - Double.valueOf(data.get(beginIndex).get("DRYK").toString());
            qmzc = Double.valueOf(data.get(endIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(endIndex).get("ZFZ").toString());
            objects[0] = calcZzl(qczc, qmzc);
            objects[1] = "single";
            return objects;
        }
        double pjjzc = 0d;
        double zdbje = 0d;
        Object[] timeCost = getProfitRateByTimeCost(tradeDatas);
        if ((double) timeCost[0] > limit){
            zdbje = getSylZdbje(tradeDatas);
            if (zdbje > toplimit){
                pjjzc = getSylByJzc(tradeDatas);
                objects[0] = pjjzc;
                objects[1] = "pjjzc";
            }else {
                zdbje = getSylZdbje(tradeDatas);
                objects[0] = zdbje;
                objects[1] = "zdbje";
            }
        }else {
            objects[0] = timeCost[0];
            objects[1] = timeCost[1];
        }
        return objects;
    }
    public static Object[] getProfitRateByTimeCost(List<Map<String, Object>> data){
        double xirr = calcXirr(data);
        double irr = calcIrr(data);
        double ljcc = getSylByCumulativeCost(data);
        List<Double> rates = Arrays.asList(Math.abs(xirr), Math.abs(irr), Math.abs(ljcc));
        double min_ = Collections.min(rates);
        int i = rates.indexOf(min_);
        Object[] objects = new Object[2];
        if (i == 0){
            objects[0] = xirr;
            objects[1] = "xirr";
        }else if (i == 1){
            objects[0] = irr;
            objects[1] = "irr";
        }else {
            objects[0] = ljcc;
            objects[1] = "ljcc";
        }
        return objects;
    }
    /**
     * 收益率组合算法
     * 优先xirr
     * xirr超过上限计算累积成本
     * 累积成本超过上限则返回xirr
     * @param data 资产数据
     * @param limit 收益率上限
     * @return
     */
    public static double getReturnRateByMultiFunc(List<Map<String, Object>> data, double limit){
        double qczc;
        double qmzc;
        List<Object> tradeinfo = getTradeDatas(data);
        if (tradeinfo.get(0) == null){//无交易数据
            return 0d;
        }
        int beginIndex = Integer.valueOf(tradeinfo.get(1)+"").intValue();
        int endIndex = Integer.valueOf(tradeinfo.get(2)+"").intValue();
        List<Map<String, Object>> tradeDatas = (List<Map<String, Object>>)tradeinfo.get(0);
        if (interestSettleAccount(tradeDatas)){
            return getDemandIncome();
        }
        if (beginIndex == endIndex){//只有一笔交易数据
            qczc = Double.valueOf(data.get(beginIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(beginIndex).get("ZFZ").toString())
                    - Double.valueOf(data.get(beginIndex).get("DRYK").toString());
            qmzc = Double.valueOf(data.get(endIndex).get("ZZC").toString())
                    - Double.valueOf(data.get(endIndex).get("ZFZ").toString());
            //收益率
            return calcZzl(qczc, qmzc);
        }
        double pjjzc = 0d;
        double zdbje = 0d;
        Object[] timeCost = getProfitRateByTimeCost(tradeDatas);
        if ((double) timeCost[0] > limit){
            zdbje = getSylZdbje(tradeDatas);
            if (zdbje > toplimit){
                pjjzc = getSylByJzc(tradeDatas);
                return pjjzc;
            }else {
                return zdbje;
            }
        }else {
            return (double) timeCost[0];
        }
    }

    private static double getDemandIncome(){
        if (demandIncome == null){
            try{
                Properties props = new Properties();
                props.load(QuotaFunction.class.getResourceAsStream("/config.properties"));
                demandIncome = Double.valueOf(props.getProperty("ids.syl.demandIncome"));
                return demandIncome;
            }catch (Exception e){
                return 0d;
            }
        }
        return demandIncome;
    }
    /**
     * 判断结息入账异常情况
     * @param data
     * @return
     */
    public static boolean interestSettleAccount(List<Map<String, Object>> data){
        double ljyk = 0d;
        double lx = 0d;
        double qtsr = 0d;
        for (int i=0; i<data.size(); i++){
            ljyk += Double.valueOf(data.get(i).get("DRYK").toString());
            try{
                qtsr = Double.valueOf(data.get(i).get("QTSR").toString());
            }catch (Exception e){

            }
            lx += qtsr;
        }
        if (ljyk != 0d && lx != 0d){
            if (lx/ljyk > 0.8){
                return true;
            }
        }
        return false;
    }
    /**
     * 最大本金额
     * @param data
     * @return
     */
    public static double getSylZdbje(List<Map<String, Object>> data){
        double syl = 0d;
        List<Double> bje = new ArrayList<>();
        if (data.size()>0){
            double ljyk = 0d;
            bje.add(Double.valueOf(data.get(0).get("ZZC").toString())
                    - Double.valueOf(data.get(0).get("ZFZ").toString())
                    - Double.valueOf(data.get(0).get("ZCJLR").toString())
                    - Double.valueOf(data.get(0).get("DRYK").toString()));
            for (int i=0; i<data.size()-1; i++){
//                double zcjlr = Double.valueOf(data.get(i).get("ZCJLR").toString());
//                if (Double.valueOf(data.get(i).get("ZCJLR").toString()).doubleValue() != 0d){
//                    bje.add(bje.get(bje.size()-1) + zcjlr);
//                }
                bje.add(Double.valueOf(data.get(i).get("ZZC").toString())
                        - Double.valueOf(data.get(i).get("ZFZ").toString())
                        + Double.valueOf(data.get(i).get("CRJE").toString())
                        + Double.valueOf(data.get(i).get("ZRZQSZ").toString())
                        - ljyk);
                ljyk += Double.valueOf(data.get(i).get("DRYK").toString());
            }
            ljyk += Double.valueOf(data.get(data.size()-1).get("DRYK").toString());
            syl = ljyk/(Collections.max(bje));
        }

        return syl;
    }
    /**
     * 平均净资产计算收益率
     * @param data
     * @return
     */
    public static double getSylByJzc(List<Map<String, Object>> data){
        double qczc = Double.valueOf(data.get(0).get("ZZC").toString())
                - Double.valueOf(data.get(0).get("ZFZ").toString())
                - Double.valueOf(data.get(0).get("ZCJLR").toString())
                - Double.valueOf(data.get(0).get("DRYK").toString());
        double jzc = qczc;
        double ljyk = Double.valueOf(data.get(0).get("DRYK").toString());
        double validInvestCount = 1d;
        for (int i=1; i<data.size()-1; i++){
            double drjzc = Double.valueOf(data.get(i).get("ZZC").toString()) - Double.valueOf(data.get(i).get("ZFZ").toString());
            double dryk = Double.valueOf(data.get(i).get("DRYK").toString());
            double zjye = Double.valueOf(data.get(i).get("ZJYE").toString());
            if (drjzc != zjye || dryk != 0d){
                jzc += drjzc;
                ljyk += dryk;
                validInvestCount += 1;
            }
        }
        double pjzc = jzc/validInvestCount;
        if (pjzc == 0d){
            return 0d;
        }
        double syl = ljyk / pjzc;
        return syl;
    }
    /**
     * 累积成本计算收益率
     * @return
     */
    public static double getSylByCumulativeCost(List<Map<String, Object>> data){
        //期初资产
        double qczc = Double.valueOf(data.get(0).get("ZZC").toString())
                - Double.valueOf(data.get(0).get("ZFZ").toString())
                - Double.valueOf(data.get(0).get("ZCJLR").toString())
                - Double.valueOf(data.get(0).get("DRYK").toString());
        //期末资产
        double qmzc = Double.valueOf(data.get(data.size()-1).get("ZZC").toString()) - Double.valueOf(data.get(data.size()-1).get("ZFZ").toString());
        //开始日期
        int dayStart = Double.valueOf(data.get(0).get("RQ").toString()).intValue();
        //计算周期时间
        double dayCount = differentDays(dayStart, Double.valueOf(data.get(data.size()-1).get("RQ").toString()).intValue(), true);
        double ljcb = 0d;
        double ljzq = 0d;
        double ljzj = 0d;
        for (int i=0; i<data.size(); i++){
            Map<String, Object> row = data.get(i);
            double day = differentDays(dayStart, Double.valueOf(row.get("RQ").toString()).intValue(), true);
            double crje = Double.valueOf(row.get("CRJE").toString());//存入金额
            double qcje = Double.valueOf(row.get("QCJE").toString());//取出金额
            double sjbl = 1.0-day/dayCount; //有效时间比例
            ljcb += (crje-qcje) * sjbl;
            ljzj += (crje-qcje);
            ljzq += (Double.valueOf(row.get("ZRZQSZ").toString())-Double.valueOf(row.get("ZCZQSZ").toString()));
        }
        double ljtr = qczc+ljcb+ljzq;
        if (ljtr != 0){
            //(期末资产-期初资产-累计净转入-累计净托管市值)/(期初资产+累积成本+累计净托管市值)  其中累积成本为净存入*剩余周期比例
            double ljyk = qmzc-qczc-ljzj-ljzq;
            if (ljyk > 0d && ljtr < 0d){
//                return Math.abs(ljyk/ljtr);
                return toplimit + 1d;
            }
            return ljyk/ljtr;
        }else {
            return toplimit + 1d;
        }
    }
    /**
     * XIRR计算收益率
     * @param tradeDatas
     * @return
     */
    public static double calcXirr(List<Map<String, Object>> tradeDatas){
        double qczc;
        double qmzc;
        int size = tradeDatas.size();
        qczc = Double.valueOf(tradeDatas.get(0).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZFZ").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZCJLR").toString())
                - Double.valueOf(tradeDatas.get(0).get("DRYK").toString());
        qmzc = Double.valueOf(tradeDatas.get(size-1).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(size-1).get("ZFZ").toString());
        if (size <= 2){
            double jlr = 0d;
            for (int i=0; i<size; i++){
                jlr += Double.valueOf(tradeDatas.get(i).get("ZCJLR")+"");
            }
            if (jlr >= 0d && (qczc+jlr) > 0d){
                return qmzc/(qczc+jlr)-1.0;
            }else if (jlr < 0d && qczc > 0d){
                return (qmzc-jlr)/qczc-1.0;
            }else {
                return 0d;
            }
        }
        List<Double> zrzcList = new ArrayList<>();
        List<Double> dayList = new ArrayList<>();

        zrzcList.add(qczc);
        dayList.add(Double.valueOf(tradeDatas.get(0).get("RQ").toString()));
        boolean addLastDay = false;
        for (int i=0; i<size; i++){
            double zcjlr = Double.valueOf(tradeDatas.get(i).get("ZCJLR").toString());
            if (zcjlr != 0.0){
                zrzcList.add(0.0 - zcjlr);
                dayList.add(Double.valueOf(tradeDatas.get(i).get("RQ").toString()));
                if (i == size-1){
                    addLastDay = true;
                }
            }
        }

        zrzcList.add(qmzc);
        double rq = Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("RQ").toString());
        dayList.add((double)DateUtils.getNextDay((int) rq, 0, 0, 1));
//        if (addLastDay){
//            dayList.add((double)DateUtils.getNextDay((int) rq, 0, 0, 1));
//        }else {
//            dayList.add(rq);
//        }
//        dayList.add(Double.valueOf(tradeDatas.get(tradeDatas.size()-1).get("RQ").toString()));
        double syl = calcSyl(tranListToArray(dayList),
                tranListToArray(zrzcList),
                (int) dayList.get(0).doubleValue(),
                (int) dayList.get(dayList.size()-1).doubleValue());
        return syl;
    }
    /**
     * IRR计算收益率
     * @param tradeDatas
     * @return
     */
    public static double calcIrr(List<Map<String, Object>> tradeDatas){
        double qczc;
        double qmzc;
        int size = tradeDatas.size();
        qczc = Double.valueOf(tradeDatas.get(0).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZFZ").toString())
                - Double.valueOf(tradeDatas.get(0).get("ZCJLR").toString())
                - Double.valueOf(tradeDatas.get(0).get("DRYK").toString());
        qmzc = Double.valueOf(tradeDatas.get(size-1).get("ZZC").toString())
                - Double.valueOf(tradeDatas.get(size-1).get("ZFZ").toString());
        if (size <= 2){
            double jlr = 0d;
            for (int i=0; i<size; i++){
                jlr += Double.valueOf(tradeDatas.get(i).get("ZCJLR")+"");
            }
            if (jlr >= 0d && (qczc+jlr) > 0d){
                return qmzc/(qczc+jlr)-1.0;
            }else if (jlr < 0d && qczc > 0d){
                return (qmzc-jlr)/qczc-1.0;
            }else {
                return 0d;
            }
        }
        List<Double> zrzcList = new ArrayList<>();

        zrzcList.add(qczc);
        boolean addLastDay = false;
        for (int i=0; i<size; i++){
            double zcjlr = Double.valueOf(tradeDatas.get(i).get("ZCJLR").toString());
            if (zcjlr != 0.0){
                zrzcList.add(0.0 - zcjlr);
                if (i == size-1){
                    addLastDay = true;
                }
            }
        }
        zrzcList.add(qmzc);
        double syl = calcSylIrr(tranListToArray(zrzcList));
        return syl;
    }
    public static double calcStandardDeviation(double[] doubles){
        double average = calcAverage(doubles);
        double total = 0.0;
        for (int i=0; i<doubles.length; i++){
            total += (doubles[i]-average)*(doubles[i]-average);
        }
        double standarDeviation = Math.sqrt(total/doubles.length);
        return standarDeviation;
    }
    public static double calcAverage(double[] doubles){
        double sum = 0.0;
        for (int i=0; i<doubles.length; i++){
            sum += doubles[i];
        }
        double average = sum/doubles.length;
        return average;
    }
    public static double calcVariance(double[] doubles){
        double average = calcAverage(doubles);
        double total = 0.0;
        for (int i=0; i<doubles.length; i++){
            total += Math.pow(doubles[i]-average, 2);
        }
        double variance = total/doubles.length;
        return variance;
    }
    public static double calcCovariance(double[] x, double[] y){
        double covariance = 0.0;
        if (x.length != y.length || x.length == 0 || y.length == 0){
            return 0.0;
        }else {
            double total = 0.0;
            double averageX = calcAverage(x);
            double averageY = calcAverage(y);
            for (int i=0; i<x.length; i++){
                total += (x[i] - averageX) * (y[i] - averageY);
            }
            covariance = total / x.length;
            return covariance;
        }
    }

    /**
     * 计算最大回撤
     * @param data
     * @return
     */
    public static List<String> calcZdhc(List<Map<String, Object>> data){
        List<Double> jzList = new ArrayList<>();
        List<Integer> rqList = new ArrayList<>();
        for (Map<String, Object> row: data){
            jzList.add(Double.parseDouble(row.get("ZXJZ").toString()));
            rqList.add(Integer.valueOf(row.get("RQ").toString()));
        }
        double zdhcl = calcZdhcl(jzList);
        String describe = calcZdhchfts(rqList, jzList);
        List<String> list = new ArrayList<>();
        list.add(String.valueOf(new BigDecimal(zdhcl).setScale(6, BigDecimal.ROUND_HALF_UP)));
        list.add(describe);
        return list;

    }

    public static double calcBdl(double[] sylDoubles){
        double standardDeviation = calcStandardDeviation(sylDoubles);
        double bdl = standardDeviation * Math.pow(Double.valueOf(sylDoubles.length), 0.5);
        return bdl;
    }
    private static List<Integer> getIndexFromCustBills(List<Map<String, Object>> data, int ksrq, int jsrq){
        List<Integer> index = new ArrayList<>();
        int pre = Integer.valueOf(data.get(0).get("RQ").toString());
        for (int i=0; i<data.size(); i++){
            int rq = Integer.valueOf(data.get(i).get("RQ").toString());
            if (rq == ksrq || (pre < ksrq && rq > ksrq) || (i == 0 && rq > ksrq)){
                index.add(i - rq == ksrq ? 0 : 1);
            }
            if (rq == jsrq || (pre < jsrq && rq > jsrq)){
                index.add(i - rq == jsrq ? 0 : 1);
            }
            pre = rq;
        }
        return index;
    }

    public static double calcSharpRatio(double[] sylDoubles, double noRiskRate){
        double ratio = 0.0;
        double average = calcAverage(sylDoubles);
        double standardDiviation = calcStandardDeviation(sylDoubles);
        ratio = standardDiviation==0.0 ? 0.0: (average-noRiskRate)/standardDiviation;
        return ratio;
    }

    /**
     * 查询时间段内每一期的收益率
     * @param data 每日资产数据，包括RQ,ZZC,ZCJLR,DRYK
     * @param begin 查询开始日期
     * @param end 查询结束日期
     * @return double[]
     */
    public static double[] getSylDoubles(List<Map<String, Object>> data, int begin, int end){
        int ksrq = begin;
        ArrayList sylList = new ArrayList();
        int jsrq;
        do {
            jsrq = DateUtils.getNextMonth(ksrq, -1);
            if (jsrq <= end) {
                List<Integer> index = getIndexFromCustBills(data, ksrq, jsrq);
                if (index.size() == 2){
                    try{
                        List<Map<String, Object>> sub = data.subList(index.get(0), index.get(1) + 1);
                        sylList.add(getSyl(sub));
                    }catch (Exception e){
                        return null;
                    }

                }
                ksrq = DateUtils.getNextDay(ksrq, 0, 0, 1);
            }
        } while(jsrq < end);
        return tranListToArray(sylList);
    }

    /**
     * calmar指数按原则是算最近一年的收益率
     * @param syl
     * @param zdhcl
     * @param dayRun
     * @return
     */
    public static double calcCalmar(double syl, double zdhcl, int dayRun){
        double ratio = 0.0;
        ratio = dayRun == 0 && zdhcl == 0 ? 0: syl/zdhcl/dayRun*365;
        return ratio;
    }


    public static double calcBeta(double[] sylDoubles, double[] zsSylDoubles){
        double ratio = calcCovariance(zsSylDoubles, sylDoubles)/ calcVariance(zsSylDoubles);
        return ratio;
    }
    public static List<Object> getTradeDatas(List<Map<String, Object>> data){
        List<Object> list = new ArrayList<>();
        int beginIndex = 0;
        int endIndex = data.size() - 1;
        boolean findLeft = false;
        for (int i=0; i<data.size(); i++){
            Map<String, Object> row = data.get(i);
            if ((Double.valueOf(row.get("ZZC").toString()).doubleValue() != Double.valueOf(row.get("ZJYE").toString()).doubleValue())
                    || Math.abs(Double.valueOf(row.get("DRYK").toString()).doubleValue()) >= 0.1){
                beginIndex = i;
                findLeft = true;
                break;
            }
        }
        if (findLeft){
            for (int i=endIndex; i>=0; i--){
                Map<String, Object> row = data.get(i);
                if ((Double.valueOf(row.get("ZZC").toString()).doubleValue() != Double.valueOf(row.get("ZJYE").toString()).doubleValue())
                        || Math.abs(Double.valueOf(row.get("DRYK").toString()).doubleValue()) >= 0.1){
                    endIndex = i;
                    break;
                }
            }
        }
        if (findLeft){
            if (beginIndex == endIndex){//就一条有效交易数据
                list.add(data.subList(beginIndex, beginIndex+1));
            }else {
//                beginIndex -= 1;
//                beginIndex = beginIndex < 0 ? 0: beginIndex;
//                endIndex += 2;
//                endIndex = endIndex > data.size() ? data.size(): endIndex;
                list.add(data.subList(beginIndex, endIndex + 1));
            }
            list.add(beginIndex);
            list.add(endIndex);
            return list;
        }else {
            list.add(null);
            list.add(null);
            list.add(null);
            return list;
        }



    }

}

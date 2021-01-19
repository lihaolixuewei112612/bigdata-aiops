package com.apex_ids.udtfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.apex_ids.util.MyEnum.Worth;
import com.apex_ids.util.UdfUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Description(
        name = "ConvertWorth",
        extended = "折算净值"
)
public class ConvertWorth extends GenericUDTF {
    @Override
    public StructObjectInspector initialize(ObjectInspector[] inOIs) throws UDFArgumentException {
        if (inOIs.length < 1){
            throw new UDFArgumentException("没有数据咯");
        }
        return putFormat();
    }

    @Override
    public void process(Object[] args) throws HiveException {
        List input = Arrays.asList(args);
        List<Object> row = tranDataType((List<String>) input);
        Object[] result = cost(row);
        forward(result);
    }

    @Override
    public void close() throws HiveException {

    }

    private Object[] cost(List<Object> row){
        try{
            boolean init = false;
            boolean quickTrade = false;
            int reload = 0;
            double zzc = (double)row.get(Worth.in.zzc.getIndex());
            double zfz = (double)row.get(Worth.in.zfz.getIndex());
            double crje = (double)row.get(Worth.in.crje.getIndex());
            double zrzqsz = (double)row.get(Worth.in.zrzqsz.getIndex());
            double qcje = (double)row.get(Worth.in.qcje.getIndex());
            double zczqsz = (double)row.get(Worth.in.zczqsz.getIndex());
            double zcjlr = crje + zrzqsz - qcje - zczqsz;
            double dryk = (double)row.get(Worth.in.yk.getIndex());
            double srfe = (double)row.get(Worth.in.srfe.getIndex());
            double srjz = (double)row.get(Worth.in.srjz.getIndex());
            double srzc = zzc - zfz - zcjlr - dryk;
            double jzc = zzc-zfz;
            JSONObject cache = JSON.parseObject(row.get(Worth.in.cache.getIndex()) == null ? "" : row.get(Worth.in.cache.getIndex()).toString());
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
            if (cache == null){
                if (dryk == 0d){
                    cache = initCostCache(new BigDecimal(jzc).setScale(2, RoundingMode.HALF_UP));
                    Object[] result = new Object[]{row.get(Worth.in.khh.getIndex()), jzc, 1.0, 1.0, 0.0,0, cache.toJSONString()};
                    return result;
                }else {
                    BigDecimal qcfe = new BigDecimal(crje+"").add(new BigDecimal(zrzqsz+"")).setScale(2, RoundingMode.HALF_UP);
                    cache = initCostCache(qcfe);
                }
                init = true;
            }
            if (cache.getIntValue("reload") == 3 || cache.getIntValue("reload") == 1 || srjz == 0d){
                srjz = 1.0d;
            }
            if (cache.getIntValue("reload") == 1){
                cache = reloadCostCache(cache,
                        new BigDecimal(srzc).setScale(2, RoundingMode.HALF_UP),
                        new BigDecimal(srfe).setScale(2, RoundingMode.HALF_UP),
                        new BigDecimal(dryk).setScale(2, RoundingMode.HALF_UP)
                );
            }
            if (cache.getIntValue("reload") == 3){
                // 20201105 全部资金取出后，利息收入到账，发生折算后，qcfe为0，故取上日份额为期初份额
                cache = initCostCache(new BigDecimal(srfe).setScale(2, RoundingMode.HALF_UP));
            }
            if (!init){
                cache.put("ljcr", cache.getBigDecimal("ljcr").add(new BigDecimal(crje+"")).setScale(2, RoundingMode.HALF_UP));
                cache.put("ljzr", cache.getBigDecimal("ljzr").add(new BigDecimal(zrzqsz+"")).setScale(2, RoundingMode.HALF_UP));
            }
            cache.put("ljqc", cache.getBigDecimal("ljqc").add(new BigDecimal(qcje+"")).setScale(2, RoundingMode.HALF_UP));
            cache.put("ljzc", cache.getBigDecimal("ljzc").add(new BigDecimal(zczqsz+"")).setScale(2, RoundingMode.HALF_UP));
            cache.put("dqyk", cache.getBigDecimal("dqyk").add(new BigDecimal(dryk)).setScale(2, RoundingMode.HALF_UP));
            cache.put("ljyk", cache.getBigDecimal("ljyk").add(new BigDecimal(dryk)).setScale(2, RoundingMode.HALF_UP));
            double ljjlr = cache.getDouble("ljcr") + cache.getDouble("ljzr") - cache.getDouble("ljqc") - cache.getDouble("ljzc");
            double qczs = (cache.getDouble("ljcr")+cache.getDouble("ljzr")+cache.getDouble("qcfe")) == 0d ? 0d: (cache.getDouble("ljqc")+cache.getDouble("ljzc"))/(cache.getDouble("ljcr")+cache.getDouble("ljzr")+cache.getDouble("qcfe"));
            double crzs = cache.getDouble("qcfe") == 0D ? 0d: (cache.getDouble("ljcr")+cache.getDouble("ljzr")-cache.getDouble("ljqc")-cache.getDouble("ljzc"))/cache.getDouble("qcfe");
            BigDecimal dqjz, dqfe;
            double jzzzl;
            double qtsr = (double)row.get(Worth.in.qtsr.getIndex());
            if (quickTrade){ //快速交易
                jzzzl = dryk/(srzc+crje+zrzqsz);
                double zxjz = 1.0 + jzzzl;
                dqjz = new BigDecimal(zxjz).setScale(4, RoundingMode.HALF_UP);
                dqfe = new BigDecimal(jzc).setScale(4, RoundingMode.HALF_UP);
                cache.put("reload", 1);
                reload = 1;
            }else if (cache.getDouble("qcfe") == 0d && zcjlr > 0d){//开始赚钱进入
                double zxfe = zcjlr;
                double zxjz = (cache.getDouble("ljqc") + cache.getDouble("ljzc") + jzc) / (cache.getDouble("ljcr") + cache.getDouble("ljzr"));
                dqjz = new BigDecimal(zxjz).setScale(4, RoundingMode.HALF_UP);
                dqfe = new BigDecimal(zxfe).setScale(2, RoundingMode.HALF_UP);
                jzzzl = zxjz - 1.0;
                cache = initCostCache(dqfe);
                cache.put("dqyk", (new BigDecimal(dryk)).setScale(2, RoundingMode.HALF_UP));
                cache.put("reload", 2);
                reload = 2;
            }else if (qtsr == dryk && qtsr >= srzc * 0.03 && qtsr > 0d && qtsr >= zzc * 0.03){ //利息收入异常
                dqjz = new BigDecimal(srjz * 1.03).setScale(4, RoundingMode.HALF_UP);
                dqfe = new BigDecimal(jzc).setScale(2, RoundingMode.HALF_UP);
                jzzzl = 0.03;
                cache.put("reload", 3);
                reload = 3;
            }else if ((qczs >= 0.5 && ljjlr < 0d) || (crzs >= 0.5 && ljjlr > 0d)){//折算
                double zxfe_pre = new BigDecimal(srfe).setScale(2, RoundingMode.HALF_UP).doubleValue();
                double zxfe_aft = new BigDecimal(srfe + (crje + zrzqsz - qcje - zczqsz) / srjz).setScale(2, RoundingMode.HALF_UP).doubleValue();
                double zxfe_lr = new BigDecimal(srfe + (crje + zrzqsz) / srjz).setScale(2, RoundingMode.HALF_UP).doubleValue();
                double zxjz_pre = new BigDecimal(zxfe_pre == 0d ? srjz : jzc/zxfe_pre).setScale(4, RoundingMode.HALF_UP).doubleValue();
                double zxjz_aft = new BigDecimal(zxfe_aft == 0d ? srjz : jzc/zxfe_aft).setScale(4, RoundingMode.HALF_UP).doubleValue();
                double zxjz_lr = new BigDecimal(zxfe_lr == 0d ? srjz : jzc /zxfe_lr).setScale(4, RoundingMode.HALF_UP).doubleValue();
                double zxjz_zc = new BigDecimal(srjz == 0d || srfe == 0d ? srjz : ((0.0-zcjlr)/srjz+jzc)/srfe).setScale(4, RoundingMode.HALF_UP).doubleValue();
                double zxjz_zce = new BigDecimal(srjz == 0d || srfe == 0d ? srjz : ((0.0-zcjlr)+jzc)/srfe).setScale(4, RoundingMode.HALF_UP).doubleValue();
                double[] jz_l = new double[]{zxjz_aft, zxjz_pre, zxjz_lr, zxjz_zc, zxjz_zce};
                double[] jzzzl_l = new double[]{zxjz_aft-srjz, zxjz_pre-srjz, zxjz_lr-srjz, zxjz_zc-srjz, zxjz_zce-srjz};
                double zxjz = jz_l[0];
                jzzzl = Math.abs(jzzzl_l[0]);
                for (int i=1; i<jzzzl_l.length; i++){
                    if (jzzzl_l[i] * dryk >= 0d){
                        if (Math.abs(jzzzl_l[i]) < jzzzl){
                            jzzzl = Math.abs(jzzzl_l[i]);
                            zxjz = jz_l[i];
                        }
                    }
                }
                dqjz = new BigDecimal(zxjz).setScale(4, RoundingMode.HALF_UP);
                dqfe = new BigDecimal(jzc).setScale(4, RoundingMode.HALF_UP);
                jzzzl = zxjz/srjz-1.0;
                cache.put("reload", 1);
                reload = 1;
            }else {//正常
                double zxfe = new BigDecimal(srfe + (crje + zrzqsz - qcje - zczqsz) / srjz).setScale(2, RoundingMode.HALF_UP).doubleValue();
                double zxjz = zxfe == 0d ? 1.0 : jzc/zxfe;
                dqjz = new BigDecimal(zxjz).setScale(4, RoundingMode.HALF_UP);
                dqfe = new BigDecimal(zxfe).setScale(2, RoundingMode.HALF_UP);
                jzzzl = zxjz/srjz - 1.0;
                cache.put("reload", 0);
                reload = 0;
            }
            cache.put("dqzs", new BigDecimal(ljjlr <= 0.0 ? qczs : -Math.abs(crzs)).setScale(2, RoundingMode.HALF_UP));
            BigDecimal gdjz = new BigDecimal((double)row.get(Worth.in.gdjz.getIndex()) * (1.0+jzzzl)).setScale(4, RoundingMode.HALF_UP);
            Object[] result = new Object[]{row.get(Worth.in.khh.getIndex()), dqfe.doubleValue(), dqjz.doubleValue(), gdjz.doubleValue(), jzzzl,reload, cache.toJSONString()};
            return result;
        }catch (Exception e){
            System.out.println("-----------异常----------");
            System.out.println("客户号：" + row.get(Worth.in.khh.getIndex()));
            e.printStackTrace();
            throw e;
//            double jzc = (double)row.get(Worth.in.zzc.getIndex())-(double)row.get(Worth.in.zfz.getIndex());
//            return new Object[]{row.get(Worth.in.khh.getIndex()), jzc, 1.0, (double)row.get(Worth.in.gdjz.getIndex()), 0.0, ""};
        }

    }
    private JSONObject initCostCache(BigDecimal qczc){
        JSONObject cache = new JSONObject();
        cache.put("ljcr", BigDecimal.ZERO);
        cache.put("ljqc", BigDecimal.ZERO);
        cache.put("ljzc", BigDecimal.ZERO);
        cache.put("ljzr", BigDecimal.ZERO);
        cache.put("ljyk", BigDecimal.ZERO);
        cache.put("qcfe", qczc);
        cache.put("dqyk", BigDecimal.ZERO);
        cache.put("dqzs", BigDecimal.ZERO);
        cache.put("reload", 0);
        return cache;
    }
    private JSONObject reloadCostCache(JSONObject cache, BigDecimal qczc, BigDecimal srfe, BigDecimal dryk){
        JSONObject newCache = new JSONObject();
        newCache.put("ljcr", BigDecimal.ZERO);
        newCache.put("ljqc", BigDecimal.ZERO);
        newCache.put("ljzc", BigDecimal.ZERO);
        newCache.put("ljzr", BigDecimal.ZERO);
        newCache.put("ljyk", cache.getBigDecimal("ljyk"));
        if (cache.getIntValue("reload") != 2){
            newCache.put("dqyk", BigDecimal.ZERO);
            newCache.put("qcfe", qczc);
        }else {
            newCache.put("dqyk", dryk);
            newCache.put("qcfe", srfe);
        }
        newCache.put("dqzs", BigDecimal.ZERO);
        newCache.put("reload", 0);
        return newCache;
    }
    private List<Object> tranDataType(List<String> row){
        /**
         * 字段格式标准化
         */
        List<Object> result = new ArrayList<>();
        for (int i=0; i<row.size(); i++){
            Object object = row.get(i);
            if (object == null){
                object = "";
            }else if (object instanceof HiveDecimalWritable){
                object = ((HiveDecimalWritable)object).getHiveDecimal().doubleValue();
            }else if (object instanceof DoubleWritable){
                object = ((DoubleWritable)object).toString();
            }else {
                object = object.toString();
            }
            result.add(UdfUtil.parseObject(object+"", Worth.in.values()[i].getType()));
        }
        return result;
    }
    private StandardStructObjectInspector putFormat(){
        /**
         * 设置输出格式
         */

        ArrayList<String> fieldNames = new ArrayList<>();
        ArrayList<ObjectInspector> outOIS = new ArrayList<>();
        for (Worth.out conf: Worth.out.values()){
            fieldNames.add(conf.toString());
            outOIS.add(UdfUtil.getInspector(conf.getType()));
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, outOIS);
    }
}

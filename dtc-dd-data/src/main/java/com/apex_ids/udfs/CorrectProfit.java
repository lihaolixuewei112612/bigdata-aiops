package com.apex_ids.udfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;

/**
 * drop function IF EXISTS udfs.CorrectProfit;
 * create FUNCTION udfs.CorrectProfit as 'com.apex.ids.udfs.CorrectProfit' USING JAR 'hdfs://master:8020/user/apex/udfs-all-1.0.0.jar';
 *
 * select khh,yk,yl_zqpz_list,ks_zqpz_list,
 * udfs.CorrectProfit(yl_zqpz_list,ks_zqpz_list,cast(yk as double),0) as yl1,
 * udfs.CorrectProfit(yl_zqpz_list,ks_zqpz_list,cast(yk as double),1) as ks1
 * from cust.t_stat_zd_y where yf = 202003 and yk != 0.0 limit 1000;
 */
public class CorrectProfit extends UDF {
    public Text evaluate(String yl_str, String ks_str, Double sy, int correct_pos){
        return call(yl_str, ks_str, sy, correct_pos);
    }
    public Text evaluate(String yl_str, String ks_str, BigDecimal sy, int correct_pos){
        return call(yl_str, ks_str, sy.doubleValue(), correct_pos);
    }
    public Text evaluate(Text yl_str, Text ks_str, Double sy, int correct_pos){
        String yl_tmp = yl_str == null ? "" : yl_str.toString();
        String ks_tmp = ks_str == null ? "" : ks_str.toString();
        return call(yl_tmp, ks_tmp, sy, correct_pos);
    }
    public Text evaluate(Text yl_str, Text ks_str, BigDecimal sy, int correct_pos){
        String yl_tmp = yl_str == null ? "" : yl_str.toString();
        String ks_tmp = ks_str == null ? "" : ks_str.toString();
        return call(yl_tmp, ks_tmp, sy.doubleValue(), correct_pos);
    }
    private static Text call(String yl_str, String ks_str, Double sy, int correct_pos){
        String yl_list = getString(yl_str);
        String ks_list = getString(ks_str);
        JSONObject yl = JSON.parseObject(yl_list);
        JSONObject ks = JSON.parseObject(ks_list);
        if (StringUtils.isBlank(yl_str)){
            yl = new JSONObject();
        }
        if (StringUtils.isBlank(ks_str)){
            ks = new JSONObject();
        }
        double yl_sum = 0.0;
        double ks_sum = 0.0;
        for (String key: yl.keySet()){
            yl_sum += getDouble(getString(yl, key));
        }
        for (String key: ks.keySet()){
            ks_sum += getDouble(getString(ks, key));
        }
        System.out.println("品种收益：" + yl_sum);
        System.out.println("品种亏损：" + ks_sum);
        double pc = sy - (yl_sum + ks_sum);
        if (pc != 0){
            if (correct_pos == 1){
                if (pc < 0d){
                    double tmp = getDouble(getString(ks, "其他"));
                    ks.put("其他", new BigDecimal(pc+tmp).setScale(2, BigDecimal.ROUND_HALF_UP));
                }
            }else {
                if (pc > 0d){
                    double tmp = getDouble(getString(yl, "其他"));
                    yl.put("其他", new BigDecimal(pc+tmp).setScale(2, BigDecimal.ROUND_HALF_UP));
                }
            }
        }
        if (correct_pos == 1){
            if (ks.isEmpty()){
                return new Text("");
            }else {
                return new Text(ks.toJSONString());
            }
        }else {
            if (yl.isEmpty()){
                return new Text("");
            }else {
                return new Text(yl.toJSONString());
            }
        }
    }
    private static String getString(String s){
        if (StringUtils.isBlank(s)){
            return "";
        }
        return s;
    }
    private static Double getDouble(String s){
        if (StringUtils.isBlank(s)){
            return 0d;
        }else {
            return Double.parseDouble(s);
        }
    }
    private static String getString(JSONObject object, String key){
        if (object == null){
            return "";
        }else{
            if (object.isEmpty()){
                return "";
            }
            Object s = object.get(key);
            if (s == null){
                return "";
            }
            return object.get(key).toString();
        }
    }
//    public static void main(String[] args) {
//        String yl_str = "{\"股票\":\"39595.53\",\"其他\":\"0.0\",\"债券\":\"31.1\",\"理财\":\"2166.49\"}";
//        String ks_str = "{\"其他\":\"-8.24\",\"债券\":\"-4.0\",\"理财\":\"0.0\",\"股票\":\"-12515.64\"}";
//        Double sy = 29336.7;
//        Text y1 = call(yl_str, ks_str, sy, 0);
//        System.out.println(y1.toString());
//        sy = 28336.7;
//        Text k1 = call(yl_str, ks_str, sy, 1);
//        System.out.println(k1.toString());
//    }
}

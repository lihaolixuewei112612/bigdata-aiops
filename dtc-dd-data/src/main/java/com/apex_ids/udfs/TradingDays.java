package com.apex_ids.udfs;

import com.google.common.primitives.Ints;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;

/**
 * @author linqz
 */
@Description(
        name = "TradingDays",
        value = "_FUNC_(date1,N) - 返回整数型（yyyyMMdd）日期前（N<0)/后(N>0) N个交易日 "
)
public class TradingDays extends UDF {
    private static final Logger log = LoggerFactory.getLogger(DaysBetween.class);

    //--直接使用int数组，搜索更方便高效
    private static int[] arZrr;
    private static int[] arJyr;
    private static long tmLoad;
    public static void initCache(){
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
    static {
        initCache();
    }

    public IntWritable evaluate(HiveDecimalWritable dateStr1, IntWritable iDays) {
        if (dateStr1 == null) {
            return null;
        }
        int ndate = Integer.parseInt(dateStr1+"");
        int days = getDays(iDays);
        return new IntWritable(findTradingDay(ndate, days));
    }
    public IntWritable evaluate(Text dateStr1, IntWritable iDays) {
        if (dateStr1 == null) {
            return null;
        }
        int ndate = Integer.parseInt(dateStr1.toString());
        int days = getDays(iDays);
        return new IntWritable(findTradingDay(ndate, days));
    }

    public IntWritable evaluate(Decimal dateStr1, IntWritable iDays) {
        if (dateStr1 == null) {
            return null;
        }
        int ndate = Integer.parseInt(dateStr1+"");
        int days = getDays(iDays);
        return new IntWritable(findTradingDay(ndate, days));
    }
    public IntWritable evaluate(DateWritable dw1, IntWritable iDays) {
        if (dw1 == null) {
            return null;
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dw1.get());
        int ndate = calendar.get(Calendar.YEAR) * 10000 + calendar.get(Calendar.MONTH) * 100 + calendar.get(Calendar.DAY_OF_MONTH);
        int days = getDays(iDays);
        return new IntWritable(findTradingDay(ndate, days));
    }


    public IntWritable evaluate(IntWritable iDate, IntWritable iDays) {
        if (iDate == null)
            return null;
        int ndate = iDate.get();
        if (ndate <= 0) {
            //--测试返回，<0 检测该函数存活了多长时间
            return new IntWritable((int) ((System.currentTimeMillis() - tmLoad) / 1000));
        }
        int days = getDays(iDays);
        return new IntWritable(findTradingDay(ndate, days));
    }

    private int getDays(IntWritable iDays) {
        int days = 0;
        if (iDays != null)
            days = iDays.get();
        return days;
    }

    public static int findTradingDay(int ndate, int days) {
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
    public static List<Integer> getTradingDays(Integer ksrq, Integer jsrq){
        int[] Zrr = TradingDays.arZrr;
        int[] Jyr = TradingDays.arJyr;
        List days = new ArrayList();
        int indexStart = Arrays.binarySearch(Zrr, ksrq);
        int indexEnd = Arrays.binarySearch(Zrr, jsrq);

        if (Zrr[indexStart] != Jyr[indexStart]){
            indexStart = Arrays.binarySearch(Zrr, TradingDays.findTradingDay(ksrq, 1));
        }
        for (int idx=indexStart; idx<=indexEnd; idx++){
            if (Zrr[idx] == Jyr[idx]){
                days.add(Jyr[idx]);
            }
        }
        return days;
    }

}

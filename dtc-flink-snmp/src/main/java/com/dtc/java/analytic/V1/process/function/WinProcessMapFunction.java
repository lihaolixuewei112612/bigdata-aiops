package com.dtc.java.analytic.V1.process.function;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 2020-01-19
 *
 * @author :hao.li
 */
@Slf4j
public class WinProcessMapFunction extends ProcessWindowFunction<Tuple6<String, String, String, String, String, String>, Tuple6<String, String, String, String, String, String>, Tuple, TimeWindow> {

    Map<String, String> diskDescribe = new HashMap();
    Map<String, String> diskBlockSize = new HashMap();
    Map<String, String> diskBlockNum = new HashMap();
    Map<String, String> diskCaption = new HashMap();

    @Override
    public void process(Tuple tuple, Context context, Iterable<Tuple6<String, String, String, String, String, String>> iterable, Collector<Tuple6<String, String, String, String, String, String>> collector) throws Exception {

        int count = 0;
        double sum = 0;
        String System_name = null;
        String Host = null;
        String ZB_Name = null;
        String ZB_Code = null;
        String time = null;


        for (Tuple6<String, String, String, String, String, String> wc : iterable) {
//           String keyValue= wc.f1 + "_" + wc.f2 + "-" + wc.f3;
            String keyValue = wc.f1 + "_" + wc.f3;
            /**
             * 磁盘描述
             * */
            if ("101_100_103_103_103".equals(wc.f2)) {
                if ((!diskDescribe.containsKey(keyValue)) || (diskDescribe.containsKey(keyValue) && (!diskDescribe.get(keyValue).equals(wc.f5)))) {
                    diskDescribe.put(keyValue, wc.f5);
                    String Url = "jdbc:mysql://localhost/test01";//参数参考MySql连接数据库常用参数及代码示例
                    String name = "root";//数据库用户名
                    String psd = "psd";//数据库密码
                    String jdbcName = "com.mysql.jdbc.Driver";//连接MySql数据库
                    String sql = "insert into test values(?,?,?)";//数据库操作语句（插入）
                    Connection con = null;
                    try {
                        Class.forName(jdbcName);//向DriverManager注册自己
                        con = DriverManager.getConnection(Url, name, psd);//与数据库建立连接
                        PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                        pst.setString(1, wc.f1);
                        pst.setString(2, wc.f3);
                        pst.setString(3, wc.f5);
                        pst.executeUpdate();//解释在下
                    } catch (ClassNotFoundException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } finally {
                        if (con != null) {
                            con.close();
                        }

                    }
                }
            }
            /**
             * 每个块的大小
             * */
            if ("101_100_103_104_104".equals(wc.f2)) {
                if (!diskBlockSize.containsKey(keyValue)) {
                    diskBlockSize.put(keyValue, wc.f5);
                }
            }
            /**
             * 磁盘块的个数
             * */
            if ("101_100_103_104_105".equals(wc.f2)) {
                if (!diskBlockNum.containsKey(keyValue)) {
                    diskBlockNum.put(keyValue, wc.f5);
                }
            }


            /**
             * 每个盘的总容量
             * */
            if (getLikeByMap(diskBlockNum, wc.f1) == getLikeByMap(diskBlockSize, wc.f1)) {
                for (String keyA : diskBlockSize.keySet()) {
                    for (String keyB : diskBlockNum.keySet()) {
                        if (keyA.equals(keyB)) {
                            double valueA = Double.parseDouble(diskBlockNum.get(keyA));
                            double valueB = Double.parseDouble(diskBlockSize.get(keyB));
                            if (!diskCaption.containsKey(keyA)) {
                                diskCaption.put(keyA, String.valueOf(valueA * valueB));
                            }
                        }
                    }
                }
            }
            /**
             * 磁盘使用量
             * 磁盘使用率
             * 虚拟/物理内存使用率
             * */
            if ("101_100_103_104_106".equals(wc.f2) && diskCaption.containsKey(keyValue)) {
                Double resu = Double.parseDouble(wc.f5) * Double.parseDouble(diskBlockSize.get(keyValue));
                Double diskUsedCapacity = Double.parseDouble(diskCaption.get(keyValue));
                //磁盘使用率
                Double result = resu / diskUsedCapacity;
                collector.collect(Tuple6.of(wc.f0, wc.f1, wc.f2 + "." + wc.f3, wc.f3, wc.f4, String.valueOf(result)));
                continue;
            }

            System_name = wc.f0;
            Host = wc.f1;
            ZB_Name = wc.f2;
            ZB_Code = wc.f3;
            time = wc.f4;
            count++;
            double value = Double.parseDouble(wc.f5);
            sum += value;
        }
        double result = sum / count;
        collector.collect(new Tuple6<>(System_name, Host, ZB_Name, ZB_Code, time, String.valueOf(result)));
    }

    private int getLikeByMap(Map<String, String> map, String keyLike) {
        List<String> list = new ArrayList<>();
        for (Map.Entry<String, String> entity : map.entrySet()) {
            if (entity.getKey().startsWith(keyLike)) {
                list.add(entity.getValue());
            }
        }

        return list.size();
    }
}

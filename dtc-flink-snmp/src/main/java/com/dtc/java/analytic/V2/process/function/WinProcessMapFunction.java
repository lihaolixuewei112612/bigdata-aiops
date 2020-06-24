package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 2020-02-21
 *
 * @author :hao.li
 */
@Slf4j
public class WinProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {

    //磁盘描述
    private Map<String, String> diskDescribe = new HashMap();
    //磁盘每个块的大小
    private Map<String, String> diskBlockSize = new HashMap();
    //磁盘块的个数
    private Map<String, String> diskBlockNum = new HashMap();
    //磁盘容量
    private Map<String, String> diskCaption = new HashMap();
    //cpu的核数
    Map<String, String> cpuNum = new HashMap<>();
    Map<String, Map<String, String>> cpuSys = new HashMap<String, Map<String, String>>();
    private boolean flag = false;

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> iterable, Collector<DataStruct> collector) throws Exception {

        ParameterTool parameters = (ParameterTool)
                getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

        String userName = parameters.get(PropertiesConstants.MYSQL_USERNAME);
        String passWord = parameters.get(PropertiesConstants.MYSQL_PASSWORD);
        String host = parameters.get(PropertiesConstants.MYSQL_HOST);
        String port = parameters.get(PropertiesConstants.MYSQL_PORT);
        String database = parameters.get(PropertiesConstants.MYSQL_DATABASE);
        String mysql_win_table_sql = parameters.get(PropertiesConstants.MYSQL_WINDOWS_TABLE);
        double cpu_sum = 0;
        double net_num = 0;
        double rec_total = 0;
        double rec_total_count = 0;
        double sent_total = 0;
        double sent_total_count = 0;
        double discard_package_in_num = 0;
        double discard_in_count = 0;
        double discard_package_out_num = 0;
        double discard_out_count = 0;
        double error_in_num = 0;
        double error_in_count = 0;
        double error_out_num = 0;
        double error_out_count = 0;
        double cpuCount = 0;
        int count = 0;
        Map<String, String> usedDiskMap = new HashMap<>();

        for (DataStruct wc : iterable) {
            String keyValue = wc.getHost() + "_" + wc.getZbLastCode();

            /**
             *
             * 系统启动时间
             *
             * */
            if ("101_100_105_102_102".equals(wc.getZbFourName())) {
                //TODO:写hbase
                collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), wc.getValue()));
                continue;
            }

            /**
             * cpu使用率
             * */
            if ("101_100_101_101_101".equals(wc.getZbFourName())) {
                if (!cpuNum.containsKey(keyValue)) {
                    cpuNum.put(keyValue, "1");
                    {
                    }
                    continue;
                } else {
                    flag = true;
                }
            }
            List<String> list = null;
            //todo:cpu展示有问题，需要重新思考
            if (flag) {
                list = new ArrayList<>();
                for (Map.Entry<String, String> entity : cpuNum.entrySet()) {
                    if (entity.getKey().startsWith(wc.getHost())) {
                        list.add(entity.getKey());
                    }
                }
                if ("101_100_101_101_101".equals(wc.getZbFourName())) {
                    if (cpuCount + 1 != list.size()) {
                        cpuCount += 1;
                        cpu_sum += Double.parseDouble(wc.getValue());
                        continue;
                    } else {
                        if (count < 1) {
                            double result = (cpu_sum / list.size())*100;
                            collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(result)));
                            count += 1;
                            cpuNum.clear();
                            continue;
                        }
                    }
                }

            }
            /**
             * 磁盘描述：存储的目的是为了保存指标与磁盘的对应关系
             * */
            if ("101_100_103_103_103".equals(wc.getZbFourName())) {
                if ((!diskDescribe.containsKey(keyValue)) || (diskDescribe.containsKey(keyValue) && (!diskDescribe.get(keyValue).equals(wc.getValue())))) {
                    diskDescribe.put(keyValue, wc.getValue());
                    String Url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
                    String JDBCDriver = "com.mysql.jdbc.Driver";
                    Connection con = null;
                    try {
                        Class.forName(JDBCDriver);
                        con = DriverManager.getConnection(Url, userName, passWord);
                        PreparedStatement pst = con.prepareStatement(mysql_win_table_sql);
                        pst.setString(1, wc.getHost());
                        pst.setString(2, wc.getZbLastCode());
                        pst.setString(3, wc.getValue());
                        pst.executeUpdate();
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
            if ("101_100_103_104_104".equals(wc.getZbFourName())) {
                if ((!diskBlockSize.containsKey(keyValue)) || (diskBlockSize.containsKey(keyValue) && (!diskBlockSize.get(keyValue).equals(wc.getValue())))) {
                    diskBlockSize.put(keyValue, wc.getValue());
                }
            }
            /**
             * 磁盘块的个数
             * */
            if ("101_100_103_105_105".equals(wc.getZbFourName())) {
                if ((!diskBlockNum.containsKey(keyValue)) || (diskBlockNum.containsKey(keyValue) && (!diskBlockNum.get(keyValue).equals(wc.getValue())))) {
                    diskBlockNum.put(keyValue, wc.getValue());
                }
            }
            /**
             * 每个盘的总容量
             * */
            if (getLikeByMap(diskBlockNum, wc.getHost()) == getLikeByMap(diskBlockSize, wc.getHost())) {
                for (String keyA : diskBlockSize.keySet()) {
                    for (String keyB : diskBlockNum.keySet()) {
                        if (keyA.equals(keyB)) {
                            double valueA = Double.parseDouble(diskBlockNum.get(keyA));
                            double valueB = Double.parseDouble(diskBlockSize.get(keyB));
                            if ((!diskCaption.containsKey(keyA)) || (diskCaption.containsKey(keyA) && !(diskCaption.get(keyA).equals(String.valueOf(valueA * valueB))))) {
                                double result = valueA * valueB;
                                BigDecimal db = new BigDecimal(result);
                                String jieguo = db.toPlainString();
                                diskCaption.put(keyA, jieguo);
                            }
                        }
                    }
                }
            }
            /**
             * 每个磁盘使用率
             * 虚拟/物理内存使用率
             * */
            List<String> list1 = new ArrayList<>();
            for (Map.Entry<String, String> entity : diskBlockSize.entrySet()) {
                if (entity.getKey().startsWith(wc.getHost())) {
                    list1.add(entity.getKey());
                }
            }
            if ("101_100_103_106_106".equals(wc.getZbFourName()) && diskCaption.containsKey(keyValue)) {
                Double used_disk = Double.parseDouble(wc.getValue()) * Double.parseDouble(diskBlockSize.get(keyValue));
                Double diskUsedCapacity = Double.parseDouble(diskCaption.get(keyValue));
                //磁盘使用率
                if (diskUsedCapacity == 0) {
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), "101_100_103_107_107", wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(0)));
                }else {
                    double rato_used_disk = (used_disk / diskUsedCapacity)*100;
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), "101_100_103_107_107", wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(rato_used_disk)));
                }
                usedDiskMap.put(keyValue, wc.getValue());

                Double re = 0.0;
                Double res = 0.0;
                if (usedDiskMap.size() == list1.size()) {
                    for (String s : usedDiskMap.values()) {
                        re += Double.parseDouble(s);
                    }
                    for (String s1 : diskCaption.values()) {
                        res += Double.parseDouble(s1);
                    }
                    if(res==0){
                        collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), "101_100_103_108_108", "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(0)));
                    }else {
                        double zonghe_disk_rate = (re / res)*100;
                        BigDecimal db = new BigDecimal(zonghe_disk_rate);
                        String result = db.toPlainString();
                        collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), "101_100_103_108_108", "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), result));
                    }
                }
                continue;
            }

            /**
             *
             * 网络接口相关
             * */
            if ("101_100_104_101_101".equals(wc.getZbFourName())) {
                net_num = Double.valueOf(wc.getValue());
                collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), wc.getValue()));
                continue;
            }
            if ("101_100_104_102_102".equals(wc.getZbFourName())) {
                rec_total += Double.valueOf(wc.getValue());
                rec_total_count += 1;
                if (rec_total_count == net_num) {
                    double result = rec_total/(1024*1024);
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(result)));
                }
            } else if ("101_100_104_103_103".equals(wc.getZbFourName())) {
                sent_total += Double.valueOf(wc.getValue());
                sent_total_count += 1;
                if (sent_total_count == net_num) {
                    double result = sent_total/(1024*1024);
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(result)));
                }
            } else if ("101_100_104_104_104".equals(wc.getZbFourName())) {
                discard_package_in_num += Double.valueOf(wc.getValue());
                discard_in_count += 1;
                if (discard_in_count == net_num) {
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(discard_package_in_num)));
                }
            } else if ("101_100_104_105_105".equals(wc.getZbFourName())) {
                error_in_num += Double.valueOf(wc.getValue());
                error_in_count += 1;
                if (error_in_count == net_num) {
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(error_in_num)));
                }
            } else if ("101_100_104_106_106".equals(wc.getZbFourName())) {
                discard_package_out_num += Double.valueOf(wc.getValue());
                discard_out_count += 1;
                if (discard_out_count == net_num) {
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(discard_package_out_num)));
                }
            } else if ("101_100_104_107_107".equals(wc.getZbFourName())) {
                error_out_count += Double.valueOf(wc.getValue());
                error_out_num += 1;
                if (error_out_num == net_num) {
                    collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), "", wc.getNameCN(), wc.getNameEN(), wc.getTime(), String.valueOf(error_out_count)));
                }
            }else if("107_107_101_101_101".equals(wc.getZbFourName())){
                collector.collect(new DataStruct(wc.getSystem_name(), wc.getHost(), wc.getZbFourName(), wc.getZbLastCode(), wc.getNameCN(), wc.getNameEN(), wc.getTime(), wc.getValue()));
            }
        }
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

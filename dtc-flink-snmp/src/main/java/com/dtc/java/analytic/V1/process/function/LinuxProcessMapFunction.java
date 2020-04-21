package com.dtc.java.analytic.V1.process.function;

import com.dtc.java.analytic.V1.snmp.DataStruct;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created on 2019-12-23
 *
 * @author :hao.li
 */
@Slf4j
public class LinuxProcessMapFunction extends ProcessWindowFunction<DataStruct, DataStruct, Tuple, TimeWindow> {
    Map netCarNum = new HashMap<String, Integer>();

    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> elements, Collector<DataStruct> collector)
            throws Exception {
        DecimalFormat df = new DecimalFormat(".0000");

        Map cpuMap = new HashMap<String, String>();
        Map swapMap = new HashMap<String, String>();
        //网络接口数量
        int netCardNumber = 0;
        //网卡发包总数的数组
        Map net_bytes_sent = new HashMap<String, String>();
        //网卡发包总数的数组
        Map net_bytes_recv = new HashMap<String, String>();
        //网卡收包错误数量的数组
        Map net_err_in = new HashMap<String, String>();
        //网卡发包错误数量的数组
        Map net_err_out = new HashMap<String, String>();
        //网卡收 丢包数量的数组
        Map net_drop_in = new HashMap<String, String>();
        //网卡发 丢包数量的数组
        Map net_drop_out = new HashMap<String, String>();
        //网卡发包数量的数组
        Map net_packets_sent = new HashMap<String, String>();
        //网卡发包数量的数组
        Map net_packets_recv = new HashMap<String, String>();

        for (DataStruct in : elements) {

            //判断是否是数据
            boolean strResult = in.getValue().matches("-?[0-9]+.*[0-9]*");
            if (!strResult) {
                log.info("value is not number of string!" + in.getValue());
            } else {
                /**
                 *  主机系统参数：系统启动时间
                 */
                if ("101_101_101_106_106".equals(in.getZbFourName())) {
                    if (in.getTime().contains(",")) {
                        String day = in.getTime().split(",")[0];
                        String result = day.split("\\s+")[0];
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), result));
                        continue;
                    } else {
                        String hour = in.getTime().split(":")[0];
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), hour));
                        continue;
                    }
                }

                /**
                 *   主机网络接口状态：获取网络接口数量
                 */
                if ("101_101_101_109_109".equals(in.getZbFourName())) {
                    netCardNumber = Integer.parseInt(in.getValue());
                    netCarNum.put(in.getHost(), netCardNumber);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
                }
                /**
                 * cpu的平均load值:1min/5min/15min
                 * */
                if ("101_101_101_101_101".equals(in.getZbFourName()) || "101_101_101_102_102".equals(in.getZbFourName()) || "101_101_101_103_103".equals(in.getZbFourName())) {
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
                }


                //网卡发包总数(M)
                if ("101_101_103_101_101".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_bytes_sent.size() != num) {
                            net_bytes_sent.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_bytes_sent.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_bytes_sent.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_bytes_sent.clear();
                            continue;
                        }
                    }
                }
                //网卡收包总数(M)
                if ("101_101_103_103_103".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_bytes_recv.size() != num) {
                            net_bytes_recv.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_bytes_recv.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_bytes_recv.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_bytes_recv.clear();
                            continue;
                        }
                    }
                }
                //网卡发包总数量（个）
                if ("101_101_103_105".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    } else{
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_packets_sent.size() != num * 2) {
                        net_packets_sent.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                        continue;
                    } else {
                        Set<String> set = net_packets_sent.keySet();
                        double result = 0;
                        for (String key : set) {
                            result += Double.parseDouble(net_packets_sent.get(key).toString());
                        }
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                        net_packets_sent.clear();
                        continue;
                    }
                }
                }
                //网卡收包总数量（个）
                if ("101_101_103_106".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_packets_recv.size() != num * 2) {
                            net_packets_recv.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_packets_recv.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_packets_recv.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_packets_recv.clear();
                            continue;
                        }
                    }
                }
                //网卡收包错误数量（个）
                if ("101_101_103_107_109".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_err_in.size() != num) {
                            net_err_in.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_err_in.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_err_in.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_err_in.clear();
                            continue;
                        }
                    }
                }
                //网卡发包错误数量（个）
                if ("101_101_103_108_110".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_err_out.size() != num) {
                            net_err_out.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_err_out.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_err_out.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result + ""));
                            net_err_out.clear();
                            continue;
                        }
                    }
                }
                //网卡收丢包数量（个）
                if ("101_101_103_109_111".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    }else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_drop_in.size() != num) {
                            net_drop_in.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_drop_in.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_drop_in.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result + ""));
                            net_drop_in.clear();
                            continue;
                        }
                    }
                }
                //网卡发丢包数量（个）
                if ("101_101_103_110_112".equals(in.getZbFourName())) {
                    if (0 == Integer.parseInt(netCarNum.get(in.getHost()).toString())) {
                        continue;
                    } else {
                        int num = Integer.parseInt(netCarNum.get(in.getHost()).toString());
                        if (net_drop_out.size() != num) {
                            net_drop_out.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_drop_out.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_drop_out.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result + ""));
                            net_drop_in.clear();
                            continue;
                        }
                    }
                }

                /**
                 *  主机内存:内存处理逻辑 包括内存总量/空闲内存/已用内存/内存使用率/内存空闲率
                 */

                if ("101_101_104_101_101".equals(in.getZbFourName())) {
                    if (cpuMap.containsKey("101_101_104_105_105") && cpuMap.containsKey("101_101_104_103_103") && cpuMap.containsKey("101_101_104_104_104")) {
                        //原始内存数据，单位为Kb
                        double mem_total = Double.parseDouble(in.getValue());
                        double mem_available = Double.parseDouble(cpuMap.get("101_101_104_105_105").toString());
                        double mem_buffered = Double.parseDouble(cpuMap.get("101_101_104_103_103").toString());
                        double mem_cached = Double.parseDouble(cpuMap.get("101_101_104_104_104").toString());
                        getCpuInfo(collector, df, in, mem_total, mem_available, mem_buffered, mem_cached);
                        cpuMap.remove("101_101_104_105_105");
                        cpuMap.remove("101_101_104_103_103");
                        cpuMap.remove("101_101_104_104_104");
                        continue;
                    } else {
                        cpuMap.put("101_101_104_101_101", in.getValue());
                        continue;
                    }
                }
                if ("101_101_104_105_105".equals(in.getZbFourName())) {
                    if (cpuMap.containsKey("101_101_104_101_101") && cpuMap.containsKey("101_101_104_103_103") && cpuMap.containsKey("101_101_104_104_104")) {
                        double mem_total = Double.parseDouble(cpuMap.get("101_101_104_101_101").toString());
                        double mem_available = Double.parseDouble(in.getValue());
                        double mem_buffered = Double.parseDouble(cpuMap.get("101_101_104_103_103").toString());
                        double mem_cached = Double.parseDouble(cpuMap.get("101_101_104_104_104").toString());
                        getCpuInfo(collector, df, in, mem_total, mem_available, mem_buffered, mem_cached);
                        cpuMap.remove("101_101_104_101_101");
                        cpuMap.remove("101_101_104_103_103");
                        cpuMap.remove("101_101_104_104_104");
                        continue;
                    } else {
                        cpuMap.put("101_101_104_105_105", in.getValue());
                        continue;
                    }
                }
                if ("101_101_104_103_103".equals(in.getZbFourName())) {
                    if (cpuMap.containsKey("101_101_104_101_101") && cpuMap.containsKey("101_101_104_105_105") && cpuMap.containsKey("101_101_104_104_104")) {
                        double mem_total = Double.parseDouble(cpuMap.get("101_101_104_101_101").toString());
                        double mem_available = Double.parseDouble(cpuMap.get("101_101_104_105_105").toString());
                        double mem_buffered = Double.parseDouble(in.getValue());
                        double mem_cached = Double.parseDouble(cpuMap.get("101_101_104_104_104").toString());
                        getCpuInfo(collector, df, in, mem_total, mem_available, mem_buffered, mem_cached);
                        cpuMap.remove("101_101_104_101_101");
                        cpuMap.remove("101_101_104_105_105");
                        cpuMap.remove("101_101_104_104_104");
                        continue;
                    } else {
                        cpuMap.put("101_101_104_103_103", in.getValue());
                        continue;
                    }
                }
                if ("101_101_104_104_104".equals(in.getZbFourName())) {
                    if (cpuMap.containsKey("101_101_104_101_101") && cpuMap.containsKey("101_101_104_105_105") && cpuMap.containsKey("101_101_104_103_103")) {
                        double mem_total = Double.parseDouble(cpuMap.get("101_101_104_101_101").toString());
                        double mem_available = Double.parseDouble(cpuMap.get("101_101_104_105_105").toString());
                        double mem_buffered = Double.parseDouble(cpuMap.get("101_101_104_103_103").toString());
                        double mem_cached = Double.parseDouble(in.getValue());
                        getCpuInfo(collector, df, in, mem_total, mem_available, mem_buffered, mem_cached);
                        cpuMap.remove("101_101_104_101_101");
                        cpuMap.remove("101_101_104_105_105");
                        cpuMap.remove("101_101_104_104_104");
                        continue;
                    } else {
                        cpuMap.put("101_101_104_104_104", in.getValue());
                        continue;
                    }
                }

                //空闲内存换成MB单位
                if ("101_101_104_106_106".equals(in.getZbFourName())) {
                    String result = Double.parseDouble(in.getValue()) / 1024 + "";
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), result));
                    continue;
                }

        /*
        主机swap空间
         */
                //Linux的Swap使用空间占比
                if ("101_101_105_101_101".equals(in.getZbFourName())) {
                    if (swapMap.containsKey("101_101_105_102_102")) {
                        double swap_total = Double.parseDouble(in.getValue());
                        double swap_used = Double.parseDouble(swapMap.get("101_101_105_102_102").toString());
                        //swap总量
                        String swapTotal_M = df.format(swap_total / 1024d);
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapTotal_M));

                        //swap使用量
                        String swapUsed_M = df.format(swap_used / 1024d);
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_102_102", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapUsed_M));

                        //swap空闲
                        String swapFree_M = df.format((swap_total - swap_used) / 1024d);
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_103_103", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapFree_M));

                        //swap使用率
                        String swapRate = df.format((swap_used / swap_total) * 100);
                        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_104_104", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapRate));

                        swapMap.remove("101_101_105_102_102");
                        continue;
                    } else {
                        swapMap.put("101_101_105_102_102", in.getValue());
                        continue;
                    }

                }
            }
            if ("101_101_105_102_102".equals(in.getZbFourName())) {
                if (swapMap.containsKey("101_101_105_101_101")) {
                    double swap_total = Double.parseDouble(swapMap.get("101_101_105_101_101").toString());
                    double swap_used = Double.parseDouble(in.getValue());
                    //swap总量
                    String swapTotal_M = df.format(swap_total / 1024d);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_101_101", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapTotal_M));

                    //swap使用量
                    String swapUsed_M = df.format(swap_used / 1024d);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapUsed_M));

                    //swap空闲
                    String swapFree_M = df.format((swap_total - swap_used) / 1024d);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_103_103", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapFree_M));

                    //swap使用率
                    String swapRate = df.format((swap_used / swap_total) * 100);
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_105_104_104", "000", in.getNameCN(), in.getNameEN(), in.getTime(), swapRate));

                    swapMap.remove("101_101_105_101_101");
                    continue;
                } else {
                    swapMap.put("101_101_105_101_101", in.getValue());
                    continue;
                }
            }
            /**
             * cpu
             *
             * */
            if ("101_101_106_101_101".equals(in.getZbFourName()) || "101_101_106_102_102".equals(in.getZbFourName())) {
                collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));

                continue;
            }
            if ("101_101_106_103_103".equals(in.getZbFourName())) {
                double cpuUsedRate = 100d - Double.parseDouble(in.getValue());
                String result = String.valueOf(cpuUsedRate);
                collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), result));
                continue;
            }

            /**
             * 主机磁盘
             */
            if ("101_101_107_104_104".equals(in.getZbFourName()) || "101_101_107_105_105".equals(in.getZbFourName()) || "101_101_107_106_106".equals(in.getZbFourName())) {
                String result = Double.parseDouble(in.getValue()) / 1048576 + "";
                collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), result));
                continue;
            }
            /**
             * 磁盘使用率
             * */
            if ("101_101_107_107_107".equals(in.getZbFourName())) {
                collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                continue;
            }


//                //路由器(华三 H3C MSR3610)的接受错包速率
//                if (in.f2.equals("102_101_101_102")) {
//                    if (StreamToFlink.totalMap.size() < 10) {
//                        StreamToFlink.totalMap.put("102_101_101_102" + "_" + in.f3, in.f5);
//                        continue;
//                    } else if (in.f3.equals("102")) {
//                        map.put("102_101_101_102_102", in.f5);
//                        if (map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_104") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
//                            double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
//                            double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
//                            String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
//                            map.remove("102_101_101_102_102");
//                            map.remove("102_101_101_102_103");
//                            map.remove("102_101_101_102_104");
//                            map.remove("102_101_101_102_105");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("103")) {
//                        map.put("102_101_101_102_103", in.f5);
//                        if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_104") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
//                            double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
//                            double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
//                            String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f3, result));
//                            StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
//                            map.remove("102_101_101_102_102");
//                            map.remove("102_101_101_102_103");
//                            map.remove("102_101_101_102_104");
//                            map.remove("102_101_101_102_105");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("104")) {
//                        map.put("102_101_101_102_104", in.f4);
//                        if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_105") && StreamToFlink.totalMap.size() == 10) {
//                            double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
//                            double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
//                            String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
//                            map.remove("102_101_101_102_102");
//                            map.remove("102_101_101_102_103");
//                            map.remove("102_101_101_102_104");
//                            map.remove("102_101_101_102_105");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("105")) {
//                        map.put("102_101_101_102_105", in.f5);
//                        if (map.containsKey("102_101_101_102_102") && map.containsKey("102_101_101_102_103") && map.containsKey("102_101_101_102_104") && StreamToFlink.totalMap.size() == 10) {
//                            double iflnErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_102_105").toString());
//                            double iflnErrors_t2 = Double.parseDouble(map.get("102_101_101_102_102").toString());
//                            double ifHClnUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_103").toString());
//                            double ifHCInMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_104").toString());
//                            double ifHCInBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_102_105").toString());
//                            String result = (iflnErrors_t1 - iflnErrors_t2) / (iflnErrors_t1 - iflnErrors_t2 + ifHClnUcastPkts_t1 - ifHClnUcastPkts_t2 + ifHCInMulticastPkts_t1 - ifHCInMulticastPkts_t2 + ifHCInBroadcastPkts_t1 - ifHCInBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_102_102", iflnErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_103", ifHClnUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_104", ifHCInMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_102_105", ifHCInBroadcastPkts_t2);
//                            map.remove("102_101_101_102_102");
//                            map.remove("102_101_101_102_103");
//                            map.remove("102_101_101_102_104");
//                            map.remove("102_101_101_102_105");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    }
//                }

//                //路由器(华三 H3C MSR3610)的发送错包速率
//                if (in.f2.equals("102_101_101_103")) {
//                    if (StreamToFlink.totalMap.size() < 10) {
//                        StreamToFlink.totalMap.put("102_101_101_103" + "_" + in.f3, in.f5);
//                        continue;
//                    } else if (in.f3.equals("106")) {
//                        map.put("102_101_101_103_106", in.f5);
//                        if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
//                            double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
//                            double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
//                            String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
//                            map.remove("102_101_101_103_106");
//                            map.remove("102_101_101_103_107");
//                            map.remove("102_101_101_103_108");
//                            map.remove("102_101_101_103_109");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("107")) {
//                        map.put("102_101_101_103_107", in.f5);
//                        if (map.containsKey("102_101_101_103_106") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
//                            double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
//                            double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
//                            String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
//                            map.remove("102_101_101_103_106");
//                            map.remove("102_101_101_103_107");
//                            map.remove("102_101_101_103_108");
//                            map.remove("102_101_101_103_109");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("108")) {
//                        map.put("102_101_101_103_108", in.f5);
//                        if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_106") && map.containsKey("102_101_101_103_109") && StreamToFlink.totalMap.size() == 10) {
//                            double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
//                            double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
//                            String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
//                            map.remove("102_101_101_103_106");
//                            map.remove("102_101_101_103_107");
//                            map.remove("102_101_101_103_108");
//                            map.remove("102_101_101_103_109");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    } else if (in.f3.equals("109")) {
//                        map.put("102_101_101_103_109", in.f5);
//                        if (map.containsKey("102_101_101_103_107") && map.containsKey("102_101_101_103_108") && map.containsKey("102_101_101_103_106") && StreamToFlink.totalMap.size() == 10) {
//                            double ifOutErrors_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_103_109").toString());
//                            double ifOutErrors_t2 = Double.parseDouble(map.get("102_101_101_103_106").toString());
//                            double ifHCOutUcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_107").toString());
//                            double ifHCOutMulticastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_108").toString());
//                            double ifHCOutBroadcastPkts_t2 = Double.parseDouble(map.get("102_101_101_103_109").toString());
//                            String result = (ifOutErrors_t1 - ifOutErrors_t2) / (ifOutErrors_t1 - ifOutErrors_t2 + ifHCOutUcastPkts_t1 - ifHCOutUcastPkts_t2 + ifHCOutMulticastPkts_t1 - ifHCOutMulticastPkts_t2 + ifHCOutBroadcastPkts_t1 - ifHCOutBroadcastPkts_t2) + "";
//                            collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                            StreamToFlink.totalMap.put("102_101_101_103_106", ifOutErrors_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_107", ifHCOutUcastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_108", ifHCOutMulticastPkts_t2);
//                            StreamToFlink.totalMap.put("102_101_101_103_109", ifHCOutBroadcastPkts_t2);
//                            map.remove("102_101_101_103_106");
//                            map.remove("102_101_101_103_107");
//                            map.remove("102_101_101_103_108");
//                            map.remove("102_101_101_103_109");
//                            continue;
//                        } else {
//                            continue;
//                        }
//                    }
//                }
//
//                //路由器(华三 H3C MSR3610)的接收包速率
//                if (in.f2.equals("102_101_101_104")) {
//                    if (StreamToFlink.totalMap.size() < 10) {
//                        StreamToFlink.totalMap.put("102_101_101_104" + "_" + in.f3, in.f5);
//                        continue;
//                    } else {
//                        map.put("102_101_101_104_110", in.f5);
//                        double ifHCInOctets_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_104_110").toString());
//                        double ifHCInOctets_t2 = Double.parseDouble(map.get("102_101_101_104_110").toString());
//                        String result = 8 * (ifHCInOctets_t1 - ifHCInOctets_t2) / 5000 + "";
//                        collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                        StreamToFlink.totalMap.put("102_101_101_104_110", ifHCInOctets_t2);
//                        map.remove("102_101_101_104_110");
//                        continue;
//                    }
//                }
//
//                //路由器(华三 H3C MSR3610)的发送包速率
//                if (in.f2.equals("102_101_101_105")) {
//                    if (StreamToFlink.totalMap.size() < 10) {
//                        StreamToFlink.totalMap.put("102_101_101_105" + "_" + in.f3, in.f5);
//                        continue;
//                    } else {
//                        map.put("102_101_101_105_111", in.f4);
//                        double ifHCOutOctets_t1 = Double.parseDouble(StreamToFlink.totalMap.get("102_101_101_105_111").toString());
//                        double ifHCOutOctets_t2 = Double.parseDouble(map.get("102_101_101_105_111").toString());
//                        String result = 8 * (ifHCOutOctets_t1 - ifHCOutOctets_t2) / 5000 + "";
//                        collector.collect(Tuple6.of(in.f0, in.f1, in.f2, "000", in.f4, result));
//                        StreamToFlink.totalMap.put("102_101_101_105_111", ifHCOutOctets_t2);
//                        map.remove("102_101_101_105_111");
//                        continue;
//                    }
//                }
//
//
//                //铱迅 NGFW-5731-W防火墙的内存使用率
//                if (getMessage_Two(collector, map, in, "102_104_101_101", "101", "102")) continue;
//                //铱迅 NGFW-5731-W防火墙的内存使用率
//                if (getMessage_Two(collector, map, in, "102_104_102_103", "104", "105")) continue;


            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
        }

    }


    /**
     * 主机内存:内存处理逻辑 包括内存总量/空闲内存/已用内存/内存使用率/内存空闲率
     */
    private void getCpuInfo(Collector<DataStruct> collector, DecimalFormat df, DataStruct in, double mem_total, double mem_available, double mem_buffered, double mem_cached) {
        double mem_used = mem_total - mem_available - mem_buffered - mem_cached;
        //总内存--单位MB
        String memoryTotal_M = df.format(mem_total / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_101_101", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryTotal_M));

        //可获取内存--单位MB
        String memoryAvailable = df.format(mem_available / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_105_105", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryAvailable));

        //buffer内存
        String memoryBuffer = df.format(mem_buffered / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_103_103", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryBuffer));

        //缓存内存
        String memoryCached = df.format(mem_cached / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_104_104", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryCached));


        //已用内存--单位MB
        String memoryUsed_M = df.format((mem_total - mem_available - mem_buffered - mem_cached) / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_102_102", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryUsed_M));


        //内存使用率
        String memoryUsedRate = String.valueOf(mem_used / mem_total * 100);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_109_109", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryUsedRate));

        //内存空闲率
        String memoryFreeRate = String.valueOf(100 - (mem_used / mem_total) * 100);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_110_110", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryFreeRate));

    }
}


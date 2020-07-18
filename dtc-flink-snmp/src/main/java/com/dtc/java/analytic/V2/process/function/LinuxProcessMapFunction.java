package com.dtc.java.analytic.V2.process.function;

import com.dtc.java.analytic.V2.common.model.DataStruct;
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


    @Override
    public void process(Tuple tuple, Context context, Iterable<DataStruct> elements, Collector<DataStruct> collector)
            throws Exception {
        Map<String, Integer> netCarNum = new HashMap<>();
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
            //判断数据类型是否是数值型
            boolean strResult = in.getValue().matches("-?[0-9]+.*[0-9]*");
            if (!strResult) {
                log.info("value is not number of string!" + in.getValue());
                //TODO 若数据不是数值型，可以将数据存到hbse中，例如时间
            } else {
                /**
                 *  主机系统参数：系统启动时间，这一块不会执行。等待处理
                 */
                if ("101_101_101_106_106".equals(in.getZbFourName())) {
                    //todo
//                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
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


                //网卡发送字节数(M)
                if ("101_101_103_101_101".equals(in.getZbFourName())) {

                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_bytes_sent.size() != num - 1) {
                            net_bytes_sent.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_bytes_sent.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_bytes_sent.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_bytes_sent.clear();
                            continue;
                        }
                    }
                }
                //网卡收到的字节数(M)
                if ("101_101_103_103_103".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_bytes_recv.size() != num - 1) {
                            net_bytes_recv.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_bytes_recv.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_bytes_recv.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), result / 1048576 + ""));
                            net_bytes_recv.clear();
                            continue;
                        }
                    }
                }
                //网卡发包总数量（个）
                if ("101_101_103_105_105".equals(in.getZbFourName()) || "101_101_103_105_106".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_packets_sent.size() != num * 2 - 2) {
                            net_packets_sent.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_packets_sent.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_packets_sent.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_103_105_000", "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_packets_sent.clear();
                            continue;
                        }
                    }
                }
                //网卡收包总数量（个）
                if ("101_101_103_106_107".equals(in.getZbFourName()) || "101_101_103_106_108".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_packets_recv.size() != num * 2 - 2) {
                            net_packets_recv.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_packets_recv.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_packets_recv.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_103_106_000", "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_packets_recv.clear();
                            continue;
                        }
                    }
                }
                //网卡收包错误数量（个）
                if ("101_101_103_107_109".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_err_in.size() != num - 1) {
                            net_err_in.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_err_in.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_err_in.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_err_in.clear();
                            continue;
                        }
                    }
                }
                //网卡发包错误数量（个）
                if ("101_101_103_108_110".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_err_out.size() != num - 1) {
                            net_err_out.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_err_out.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_err_out.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_err_out.clear();
                            continue;
                        }
                    }
                }
                //网卡收丢包数量（个）
                if ("101_101_103_109_111".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_drop_in.size() != num - 1) {
                            net_drop_in.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_drop_in.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_drop_in.get(key).toString());
                            }
                            result += Double.parseDouble(in.getValue());
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_drop_in.clear();
                            continue;
                        }
                    }
                }
                //网卡发丢包数量（个）
                if ("101_101_103_110_112".equals(in.getZbFourName())) {
                    int num = netCarNum.size() == 0 ? 0 : Integer.parseInt(netCarNum.get(in.getHost()).toString());
                    if (0 == num) {
                        continue;
                    } else {
                        if (net_drop_out.size() != num - 1) {
                            net_drop_out.put(in.getZbFourName() + "_" + in.getZbLastCode(), in.getValue());
                            continue;
                        } else {
                            Set<String> set = net_drop_out.keySet();
                            double result = 0;
                            for (String key : set) {
                                result += Double.parseDouble(net_drop_out.get(key).toString());
                            }
                            collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), "000", in.getNameCN(), in.getNameEN(), in.getTime(), String.valueOf(result)));
                            net_drop_out.clear();
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
                        swapMap.put("101_101_105_101_101", in.getValue());
                        continue;
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
                        swapMap.put("101_101_105_102_102", in.getValue());
                        continue;
                    }
                }
                /**
                 * cpu系统使用率/用户使用率
                 *
                 * */
                if ("101_101_106_101_101".equals(in.getZbFourName()) || "101_101_106_102_102".equals(in.getZbFourName())) {
                    collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
                    continue;
                }
                //空闲cpu使用率
                if ("101_101_106_103_103".equals(in.getZbFourName())) {
                    String result = String.valueOf(in.getValue());
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
                //多余的数据也写出去
                collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), in.getZbFourName(), in.getZbLastCode(), in.getNameCN(), in.getNameEN(), in.getTime(), in.getValue()));
            }
        }
    }


    /**
     * 主机内存:内存处理逻辑 包括内存总量/空闲内存/已用内存/内存使用率/内存空闲率
     */
    private void getCpuInfo(Collector<DataStruct> collector, DecimalFormat df, DataStruct in, double mem_total, double mem_available, double mem_buffered, double mem_cached) {
//        double mem_used = mem_total - mem_available - mem_buffered - mem_cached;
        double mem_used= mem_available;
        //总内存--单位MB
        String memoryTotal_M = df.format(mem_total / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_101_101", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryTotal_M));

        //可获取内存--单位MB
        double mem_free=mem_total-mem_used;
        String memoryAvailable = df.format(mem_free / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_105_105", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryAvailable));

        //buffer内存
        String memoryBuffer = df.format(mem_buffered / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_103_103", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryBuffer));

        //缓存内存
        String memoryCached = df.format(mem_cached / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_104_104", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryCached));


        //已用内存--单位MB
        String memoryUsed_M = df.format(mem_used / 1024d);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_102_102", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryUsed_M));


        //内存使用率
        String memoryUsedRate=null;
        double v = mem_used / mem_total * 100;
        if(v<0.0){
            memoryUsedRate ="0";
        }else {
            memoryUsedRate = String.valueOf(mem_used / mem_total * 100);
        }
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_109_109", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryUsedRate));

        //内存空闲率
        String memoryFreeRate = String.valueOf(100 - (mem_used / mem_total) * 100);
        collector.collect(new DataStruct(in.getSystem_name(), in.getHost(), "101_101_104_110_110", "000", in.getNameCN(), in.getNameEN(), in.getTime(), memoryFreeRate));

    }

}


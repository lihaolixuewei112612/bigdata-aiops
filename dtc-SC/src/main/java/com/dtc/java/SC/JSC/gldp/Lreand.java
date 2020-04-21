package com.dtc.java.SC.JSC.gldp;

import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
/*
* 驾驶舱管理大盘 source
*
* */
@Slf4j
public class Lreand extends RichSourceFunction<Map<String, String>> {
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;
    private static Connection connection = null;
    private long interval_time;

    @Override
    public void run(SourceContext<Map<String, String>> sourceContext) throws Exception {
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        interval_time = Long.parseLong(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        connection = MySQLUtil.getConnection(parameterTool);
        HashMap<String, String> map = new HashMap<String, String>();
        while(isRunning) {
            map.put("wclgd", selectA().get("wclgd"));
            map.put("jrpd", selectA().get("jrpd"));
            map.put("jrdk", selectA().get("jrdk"));
            map.put("zjbcs", selectA().get("zjbcs"));
            map.put("zjbrs", selectA().get("zjbrs"));
            map.put("jrjbcs", selectA().get("jrjbcs"));
            map.put("jrwjb", selectA().get("jrwjb"));
            map.put("yjbcs", selectA().get("yjbcs"));
            map.put("gjgd", selectA().get("gjgd"));
            map.put("tbgjgd", selectA().get("tbgjgd"));
            map.put("swgd", selectA().get("swgd"));
            map.put("tbswgd", selectA().get("tbswgd"));
            map.put("zrs", selectA().get("zrs"));


            map.put("js", selectA().get("js"));
            map.put("jsmm", selectA().get("jsmm"));
            map.put("gname", selectB().get("gname"));
            map.put("wcgd", selectB().get("wcgd"));
            map.put("gjsl", selectB().get("gjsl"));

            map.put("sjgd", selectC().get("sjgd"));
            map.put("bgsx", selectC().get("bgsx"));
            map.put("fwgd", selectC().get("fwgd"));
            map.put("gjgd", selectC().get("gjgd"));
            map.put("zysq", selectC().get("zysq"));
            map.put("wpgd", selectC().get("wpgd"));
            map.put("qt", selectC().get("qt"));
            sourceContext.collect(map);
            map.clear();
            Thread.sleep(interval_time);
        }

    }

    @Override
    public void cancel() {

    }

    private static Map<String, String> selectA() {
        PreparedStatement ps = null;
        Map<String, String> map = new HashMap<String, String>();
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        map.put("js", jisuan_riqi2);
        map.put("jsmm", jisuan_riqi1);
        Connection con = null;
        try {
            String sql = "SELECT\n" +
                    "-- 未处理工单,今日派单\n" +
                    "wclgd,jrpd,\n" +
                    "-- 今日考勤打卡到岗人数\n" +
                    "jrdk,zjbcs,zjbrs,zrs,\n" +
                    "-- 今日已加班，今日未加班\n" +
                    "jrjbcs,(jrdk-jrjbcs)as jrwjb,\n" +
                    "yjbcs,\n" +
                    "-- 告警工单(周),同比上周，事务工单，同比上周\n" +
                    "gjgd,(tgjgd-gjgd)AS tbgjgd,swgd,(tswgd-swgd)as tbswgd\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(state)from work_order WHERE work_order.state='1') as wclgd,\n" +
                    "(SELECT COUNT(state) as jr FROM work_order WHERE TO_DAYS(predict_start_time)=TO_DAYS(now())) AS jrpd,\n" +
                    "(SELECT count(k_name) as jr FROM kaoqin WHERE TO_DAYS(k_jisuan_riqi)=TO_DAYS(now())) AS jrdk,\n" +
                    "(SELECT count(j_duration)AS zjbrcs  FROM kaoqin WHERE DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(k_jisuan_riqi)AND OnResult!='null' AND j_duration!='null')AS zjbcs,\n" +
                    "(SELECT count(j_duration)AS zjbrcs  FROM kaoqin WHERE DATE_SUB(CURDATE(), INTERVAL 30 DAY) <= date(k_jisuan_riqi)AND OnResult!='null' AND j_duration!='null')AS yjbcs,\n" +
                    "(SELECT COUNT(DISTINCT (k_name)) AS zjbrs  FROM kaoqin WHERE DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(k_jisuan_riqi)AND OnResult!='null' AND j_duration!='null')AS zjbrs,\n" +
                    "(SELECT COUNT(DISTINCT user_id)FROM user_dding)AS zrs,\n" +
                    "(SELECT count(j_duration)AS zjbrcs  FROM kaoqin  WHERE TO_DAYS(k_jisuan_riqi) = TO_DAYS(NOW())AND OnResult!='null' AND j_duration!='null')AS jrjbcs,\n" +
                    "(SELECT COUNT(code)as zgd from work_order)AS zgd,\n" +
                    "(SELECT COUNT(`code`)as zgj FROM alarm)AS zgj,\n" +
                    "(SELECT COUNT(a.`status`)as gjdcl FROM alarm AS a WHERE a.`status`='0')AS gjdcl,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(create_time)AND work_order.type='4') AS gjgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(CURDATE(), INTERVAL 14 DAY) <= date(create_time)AND work_order.type='4')AS tgjgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(CURDATE(), INTERVAL 7 DAY) <= date(create_time)AND work_order.type='1')AS swgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(CURDATE(), INTERVAL 14 DAY) <= date(create_time)AND work_order.type='1')AS tswgd\n" +
                    ") as T";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                map.put("wclgd", rsq.getString("wclgd"));
                map.put("jrpd", rsq.getString("jrpd"));
                map.put("jrdk", rsq.getString("jrdk"));
                map.put("zjbcs", rsq.getString("zjbcs"));
                map.put("zjbrs", rsq.getString("zjbrs"));
                map.put("jrjbcs", rsq.getString("jrjbcs"));
                map.put("jrwjb", rsq.getString("jrwjb"));
                map.put("yjbcs", rsq.getString("yjbcs"));
                map.put("gjgd", rsq.getString("gjgd"));
                map.put("tbgjgd", rsq.getString("tbgjgd"));
                map.put("swgd", rsq.getString("swgd"));
                map.put("tbswgd", rsq.getString("tbswgd"));
                map.put("zrs", rsq.getString("zrs"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static Map<String, String> selectB() {
        PreparedStatement ps = null;
        Map<String, String> map = new HashMap<String, String>();
        Connection con = null;
        try {
            String sql = "SELECT tew.gname as gname,tew.wcgj as wcgd,count(wg.code)AS gjsl  FROM\n" +
                    "(SELECT a.code AS cod,COUNT(a.code) AS wcgj,b.name as gname\n" +
                    "FROM work_order a LEFT JOIN asset_category b ON a.code=b.code \n" +
                    "WHERE DATE_SUB(CURDATE(), INTERVAL 15 MINUTE) <= date(a.handle_finish_time)) AS tew\n" +
                    "LEFT JOIN work_order wg ON tew.cod=wg.code\n" +
                    "GROUP BY name\n";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                map.put("gname", rsq.getString("gname"));
                map.put("wcgd", rsq.getString("wcgd"));
                map.put("gjsl", rsq.getString("gjsl"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static Map<String, String> selectC() {
        PreparedStatement ps = null;
        Map<String, String> map = new HashMap<String, String>();
        Connection con = null;
        try {
            String sql = "SELECT\n" +
                    "CONCAT(CAST(ROUND((sjgd/zs*100),2) AS CHAR),'%') as sjgd,\n" +
                    "CONCAT(CAST(ROUND((bgsx/zs*100),2) AS CHAR),'%') as bgsx,\n" +
                    "CONCAT(CAST(ROUND((fwgd/zs*100),2) AS CHAR),'%') as fwgd,\n" +
                    "CONCAT(CAST(ROUND((gjgd/zs*100),2) AS CHAR),'%') as gjgd,\n" +
                    "CONCAT(CAST(ROUND((zysq/zs*100),2) AS CHAR),'%') as zysq,\n" +
                    "CONCAT(CAST(ROUND((wpgd/zs*100),2) AS CHAR),'%') as wpgd,\n" +
                    "CONCAT(CAST(ROUND((qt/zs*100),2) AS CHAR),'%') as qt\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='1' GROUP BY a.type)AS sjgd,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='2' GROUP BY a.type)AS bgsx,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='3' GROUP BY a.type)AS fwgd,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='4' GROUP BY a.type)AS gjgd,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='5' GROUP BY a.type)AS zysq,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='6' GROUP BY a.type)AS wpgd,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  WHERE a.type='7' GROUP BY a.type)AS qt,\n" +
                    "(SELECT COUNT(*)AS numb FROM work_order a  )AS zs\n" +
                    ") as T\n";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                map.put("sjgd", rsq.getString("sjgd"));
                map.put("bgsx", rsq.getString("bgsx"));
                map.put("fwgd", rsq.getString("fwgd"));
                map.put("gjgd", rsq.getString("gjgd"));
                map.put("zysq", rsq.getString("zysq"));
                map.put("wpgd", rsq.getString("wpgd"));
                map.put("qt", rsq.getString("qt"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }
}

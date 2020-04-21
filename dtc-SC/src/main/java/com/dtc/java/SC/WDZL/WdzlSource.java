package com.dtc.java.SC.WDZL;

import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

/*
 * 我的总览 source
 * */
public class WdzlSource extends RichSourceFunction<Map<String, List>> {
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;
    private static Connection connection = null;
    private long interval_time;

    @Override
    public void run(SourceContext<Map<String, List>> sourceContext) throws Exception {
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        interval_time = Long.parseLong(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        connection = MySQLUtil.getConnection(parameterTool);
        HashMap<String, List> map = new HashMap<String, List>();
        while(isRunning) {
        //工作动态
        map.put("GZDT", selectGZDT());
        //我的工单
        map.put("WDGD", selectWDGD());
        ///告警信息---没有告警标题
        // map.put("GJXX",selectGJXX());
        //工作问题
        map.put("GZWT", selectGZWT());
        //知识库分布
        map.put("ZSKFB", selectZSKFB());
        // 我处理的工单分布
        map.put("WCLDGDFB", selectWCLDGDFB());
        //项目工单近6个个月的趋势
        map.put("WCLDGJ", selectWCLDGJ());
        //资产资源分布
        map.put("ZCZYFB", selectZCZYFB());
        sourceContext.collect(map);
            map.clear();
            Thread.sleep(interval_time);

        }
    }

    private static List<Map> selectGZDT() {
        PreparedStatement ps = null;
        //工作动态
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT\n" +
                    "zgd,fpgw,hd,tw,zgj,gjdcl\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(u.job_number) FROM work_order w LEFT JOIN `user` u ON w.create_person_id=u.job_number WHERE u.job_number='4')AS zgd,\n" +
                    "(SELECT COUNT(u.job_number)AS fpgw FROM work_order w LEFT JOIN `user` u ON w.designate_id=u.job_number WHERE u.job_number='4')fpgw,\n" +
                    "(SELECT count(q.operation_desc)AS hd from question_detail AS q WHERE q.operation_desc='回答问题')AS hd,\n" +
                    "(SELECT count(q.operation_desc)as tw from question_detail AS q WHERE q.operation_desc='新建问题') as tw,\n" +
                    "(SELECT COUNT(`code`)as zgj FROM alarm)AS zgj,\n" +
                    "(SELECT COUNT(a.`status`)as gjdcl FROM alarm AS a WHERE a.`status`='0')AS gjdcl\n" +
                    ") as T";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("zgd", rsq.getString("zgd"));
                map.put("fpgw", rsq.getString("fpgw"));
                map.put("hd", rsq.getString("hd"));
                map.put("tw", rsq.getString("tw"));
                map.put("zgj", rsq.getString("zgj"));
                map.put("gjdcl", rsq.getString("gjdcl"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    //我的工单
    private static List<Map> selectWDGD() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT a.title as title ,a.type as type ,b.`name`as name,a.state as state,a.create_time as time from work_order  a LEFT JOIN `user` b ON a.create_person_id=b.id WHERE b.id='4'";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("title", rsq.getString("title"));
                map.put("type", rsq.getString("type"));
                map.put("name", rsq.getString("name"));
                map.put("state", rsq.getString("state"));
                map.put("time", rsq.getString("time"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);

                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    ///告警信息---没有告警标题
  /*  private static List<Map> selectGJXX() {
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");

        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT a.`name` as type ,b.`name` as name ,a.confirm_time as time FROM alarm a LEFT JOIN alarm_level b on a.level_id=b.`code`";//数据库操作语句（查询）
            Class.forName(MYSQL_DRIVER_NAME);//向DriverManager注册自己
            con = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PSD);//与数据库建立连接
            Statement statement = con.createStatement();
            ResultSet rsq = statement.executeQuery(sql);
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                list.add(map);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }*/

    //工作问题
    private static List<Map> selectGZWT() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");

        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT a.title as title,a.launch_person as ans,a.`name`as `name`,a.cTime as time from question a ";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("title", rsq.getString("title"));
                map.put("ans", rsq.getString("ans"));
                map.put("name", rsq.getString("name"));
                map.put("time", rsq.getString("time"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    //知识库分布
    private static List<Map> selectZSKFB() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");

        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT\n" +
                    "CONCAT(CAST(ROUND((szjy/zs*100),2) AS CHAR),'%') as szjy,\n" +
                    "CONCAT(CAST(ROUND((llzs/zs*100),2) AS CHAR),'%') as llzs,\n" +
                    "CONCAT(CAST(ROUND((pxkc/zs*100),2) AS CHAR),'%') as pxkc\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(*)AS numb FROM knowledge a  WHERE a.type='1' GROUP BY a.type)AS szjy,\n" +
                    "(SELECT COUNT(*)AS numb FROM knowledge a  WHERE a.type='2' GROUP BY a.type)AS llzs,\n" +
                    "(SELECT COUNT(*)AS numb FROM knowledge a  WHERE a.type='3' GROUP BY a.type)AS pxkc,\n" +
                    "(SELECT COUNT(*)AS numb FROM knowledge a  )AS zs\n" +
                    ") as T\n";
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("szjy", rsq.getString("szjy"));
                map.put("llzs", rsq.getString("llzs"));
                map.put("pxkc", rsq.getString("pxkc"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);


            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    //我处理的工单分布
    private static List<Map> selectWCLDGDFB() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt \n" +
                    "FROM   \n" +
                    "(SELECT   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='1'AND b.job_number='4')AS sjgd,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='2'AND b.job_number='4')AS bgsx,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='3'AND b.job_number='4')AS fwgd,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='4'AND b.job_number='4')AS gjgd,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='5'AND b.job_number='4')AS zysq,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='6'AND b.job_number='4')AS wpgd,   \n" +
                    "(SELECT COUNT(code)AS a from work_order a LEFT JOIN `user` b on a.designate_id=b.job_number WHERE a.type='7'AND b.job_number='4')AS qt  \n" +
                    ") as T";
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("sjgd", rsq.getString("sjgd"));
                map.put("bgsx", rsq.getString("bgsx"));
                map.put("fwgd", rsq.getString("fwgd"));
                map.put("gjgd", rsq.getString("gjgd"));
                map.put("zysq", rsq.getString("zysq"));
                map.put("wpgd", rsq.getString("wpgd"));
                map.put("qt", rsq.getString("qt"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    //项目工单近6个个月的趋势
    private static List<Map> selectWCLDGJ() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT a,b,c,d,e,f FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =0)AS a,\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =1)AS b ,\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =2)AS c,\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =3)AS d,\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =4)AS e,\n" +
                    "(SELECT COUNT(NAME) FROM work_order WHERE PERIOD_DIFF( date_format( now( ) , '%Y%m' ) , date_format( handle_start_time, '%Y%m' ) ) =5)AS f\n" +
                    ")AS t";
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("a", rsq.getString("a"));
                map.put("b", rsq.getString("b"));
                map.put("c", rsq.getString("c"));
                map.put("d", rsq.getString("d"));
                map.put("e", rsq.getString("e"));
                map.put("f", rsq.getString("f"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
    }

    //资产资源分布
    private static List<Map> selectZCZYFB() {
        PreparedStatement ps = null;
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi2 = timeStamp2Date(time, "yyyy-MM-dd ");
        String jisuan_riqi1 = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        ArrayList<Map> list = new ArrayList<Map>();
        Connection con = null;
        try {
            String sql = "SELECT\n" +
                    "CONCAT(CAST(ROUND((zhuji/zs*100),2) AS CHAR),'%') as zhuji, \n" +
                    "CONCAT(CAST(ROUND((wlsb/zs*100),2) AS CHAR),'%') as wlsb, \n" +
                    "CONCAT(CAST(ROUND((aqsb/zs*100),2) AS CHAR),'%') as aqsb, \n" +
                    "CONCAT(CAST(ROUND((ccsb/zs*100),2) AS CHAR),'%') as ccsb, \n" +
                    "CONCAT(CAST(ROUND((jcss/zs*100),2) AS CHAR),'%') as jcss \n" +
                    "FROM \n" +
                    "(SELECT \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z WHERE z.`name`='主机')AS zhuji, \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z WHERE z.`name`='物理设备')AS wlsb, \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z WHERE z.`name`='安全设备')AS aqsb, \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z WHERE z.`name`='存储设备')AS ccsb, \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z WHERE z.`name`='基础设施')AS jcss, \n" +
                    "(select sum(num) from \n" +
                    "(select * from \n" +
                    "(select m.zc_name,m.parent_id as pd,count(*) as num from \n" +
                    "(select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m GROUP BY m.zc_name) \n" +
                    "x left join asset_category y on x.pd = y.id) z )AS zs \n" +
                    ") as T";
            ps = connection.prepareStatement(sql);
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                Map<String, String> map = new HashMap<String, String>();
                map.put("zhuji", rsq.getString("zhuji"));
                map.put("wlsb", rsq.getString("wlsb"));
                map.put("aqsb", rsq.getString("aqsb"));
                map.put("ccsb", rsq.getString("ccsb"));
                map.put("jcss", rsq.getString("jcss"));
                map.put("js", jisuan_riqi2);
                map.put("jsmm", jisuan_riqi1);
                list.add(map);
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return list;
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

    @Override
    public void cancel() {

    }
}

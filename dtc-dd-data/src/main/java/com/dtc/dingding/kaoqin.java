package com.dtc.dingding;

import com.dtc.dingding.Model.KQinBean;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class kaoqin {
    private static final String MYSQL_URL = "jdbc:mysql://10.3.7.231:3306/real_time_alarm_data";//参数参考MySql连接数据库常用参数及代码示例
    private static final String MYSQL_NAME = "root";//数据库用户名
    private static final String MYSQL_PSD = "DTCserver2019!";//数据库密码
    private static final String MYSQL_DRIVER_NAME = "com.mysql.jdbc.Driver";//连接MySql数据库

    private List<String> result = new ArrayList<String>();

   public static void main(String[] args) {
       // public static void info() {
        List<KQinBean> list = selectAll();
        if (list != null) {
            for (KQinBean kqlist : list) {
                if (kqlist != null) {
                    writeMysql(kqlist);
                }
            }
        }

    }

    private static void writeMysql(KQinBean kqlist) {

        Connection con = null;
        try {
            String sql = "INSERT ignore INTO  kaoqin(k_userid,mobile,department,k_OnDuty,k_OffDuty,riqi,k_name,\n" +
                    "                    k_jisuan_riqi,j_duration,j_start_time,j_end_time,q_duration,\n" +
                    "                    q_start_time,q_end_time,OnResult,OffResult) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            Class.forName(MYSQL_DRIVER_NAME);//向DriverManager注册自己
            con = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PSD);//与数据库建立连接
            PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
            pst.setString(1, kqlist.getUser_id());
            pst.setString(2, kqlist.getMobile());
            pst.setString(3, kqlist.getDepartment());
            pst.setString(4, kqlist.getOnDuty());
            pst.setString(5, kqlist.getOffDuty());
            pst.setString(6, kqlist.getRiqi());
            pst.setString(7, kqlist.getName());
            pst.setString(8, kqlist.getJisuan_riqi());
            pst.setString(9, kqlist.getJ_duration());
            pst.setString(10, kqlist.getJ_start_time());
            pst.setString(11, kqlist.getJ_end_time());
            pst.setString(12, kqlist.getQ_duration());
            pst.setString(13, kqlist.getQ_start_time());
            pst.setString(14, kqlist.getQ_end_time());
            pst.setString(15, kqlist.getOnResult());
            pst.setString(16, kqlist.getOffResult());

            pst.executeUpdate();//解释在下
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static List<KQinBean> selectAll() {
        List<KQinBean> list = new ArrayList<>();
        Connection con = null;
        try {

            String sql = "SELECT u.*,j.duration AS j_duration,j.start_time AS j_start_time,j.end_time AS j_end_time,\n" +
                    "q.duration AS q_duration,q.start_time AS q_start_time,q.end_time AS q_end_time\n" +
                    "FROM user AS u LEFT JOIN jiaban AS j ON u.user_id=j.user_id AND u.jisuan_riqi=j.jisuan_riqi\n" +
                    "LEFT JOIN qingjia AS q ON u.user_id=q.user_id AND u.jisuan_riqi=q.jisuan_riqi\n";//数据库操作语句（查询）

            Class.forName(MYSQL_DRIVER_NAME);//向DriverManager注册自己
            con = DriverManager.getConnection(MYSQL_URL, MYSQL_NAME, MYSQL_PSD);//与数据库建立连接
            Statement statement = con.createStatement();
            ResultSet rsq = statement.executeQuery(sql);
            while (rsq.next()) {
                KQinBean kq = new KQinBean();
                kq.setUser_id(rsq.getString("user_id"));
                kq.setOnResult(rsq.getString("OnResult"));
                kq.setOffResult(rsq.getString("OffResult"));
                kq.setMobile(rsq.getString("mobile"));
                kq.setDepartment(rsq.getString("department"));
                kq.setOnDuty(rsq.getString("OnDuty"));
                kq.setOffDuty(rsq.getString("OffDuty"));
                kq.setRiqi(rsq.getString("riqi"));
                kq.setName(rsq.getString("name"));
                kq.setJisuan_riqi(rsq.getString("jisuan_riqi"));
                kq.setJ_duration(rsq.getString("j_duration"));
                kq.setJ_start_time(rsq.getString("j_start_time"));
                kq.setJ_end_time(rsq.getString("j_end_time"));
                kq.setQ_duration(rsq.getString("q_duration"));
                kq.setQ_start_time(rsq.getString("q_start_time"));
                kq.setQ_end_time(rsq.getString("q_end_time"));
                list.add(kq);
            }

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }

}

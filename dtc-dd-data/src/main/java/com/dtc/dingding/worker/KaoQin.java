package com.dtc.dingding.worker;

import com.dtc.dingding.Model.KQinBean;
import com.dtc.dingding.common.MySQLUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-04-26
 * @Description : TODO描述类作用
 */
public class KaoQin {

    public static void getKaoQin(Properties props) {

        List<KQinBean> list = selectAll(props);
        if (list != null) {
            for (KQinBean kqlist : list) {
                if (kqlist != null) {
                    writeToMysql(kqlist,props);
                }
            }
        }

    }

    private static void writeToMysql(KQinBean kqlist, Properties props) {

        Connection con = null;
        try {
            String sql = "replace INTO  SC_KQ(k_userid,mobile,department,k_OnDuty,k_OffDuty,riqi,k_name,\n" +
                    "                    k_jisuan_riqi,j_duration,j_start_time,j_end_time,q_duration,\n" +
                    "                    q_start_time,q_end_time,OnResult,OffResult) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            con = MySQLUtil.getConnection(props);
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

    private static List<KQinBean> selectAll(Properties props) {
        List<KQinBean> list = new ArrayList<>();
        Connection con = null;
        try {

            String sql = "SELECT u.*,j.duration AS j_duration,j.start_time AS j_start_time,j.end_time AS j_end_time,\n" +
                    "q.duration AS q_duration,q.start_time AS q_start_time,q.end_time AS q_end_time\n" +
                    "FROM SC_KQ_USER AS u LEFT JOIN SC_KQ_JB AS j ON u.user_id=j.user_id AND u.jisuan_riqi=j.jisuan_riqi\n" +
                    "LEFT JOIN SC_KQ_QJ AS q ON u.user_id=q.user_id AND u.jisuan_riqi=q.jisuan_riqi\n";//数据库操作语句（查询）

            con= MySQLUtil.getConnection(props);
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

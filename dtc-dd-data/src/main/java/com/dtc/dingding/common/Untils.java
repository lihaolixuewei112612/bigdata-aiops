package com.dtc.dingding.common;

import com.dtc.dingding.Model.JQModel;
import com.dtc.dingding.Model.SuperModel;
import com.dtc.dingding.Model.UserModel;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @Author : lihao
 * Created on : 2020-04-26
 * @Description : TODO描述类作用
 */
public class Untils {
    public static void main(String[] args) {
        Map<String, String> time = getTime_1();
        System.out.println(time);
    }
    public static Map<String, String> getTime() {
        Map<String, String> map = new HashMap<>();
        long timeStamp = System.currentTimeMillis();
        long yzero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset() - 24 * 60 * 60 * 1000;
        long zero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset();
        Calendar calendar = Calendar.getInstance();
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
        String end = String.valueOf(calendar.getTime().getTime());
        map.put("starttime", String.valueOf(yzero));
        map.put("endtime", String.valueOf(zero));
        return map;
    }

    public static Map<String, String> getTime_1() {
        Map<String, String> map = new HashMap<>();
        long timeStamp = System.currentTimeMillis();
        long zero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset() + 24 * 60 * 60 * 1000;
        long yzero = timeStamp / (1000 * 3600 * 24) * (1000 * 3600 * 24) - TimeZone.getDefault().getRawOffset();
        Calendar calendar = Calendar.getInstance();
        calendar.set(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH), 23, 59, 59);
        String end = String.valueOf(calendar.getTime().getTime());
        map.put("starttime", String.valueOf(yzero));
        map.put("endtime", String.valueOf(zero));
        return map;
    }
    public static String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }
    public static void writeMysql(String tableName, SuperModel model, Properties props) {

        Connection con = null;
        String time1 = String.valueOf(System.currentTimeMillis());
        String time = String.valueOf(System.currentTimeMillis());
        String jisuan_riqi = timeStamp2Date(time1, "yyyyMMdd");
        String system_riqi = timeStamp2Date(time, "yyyyMMdd HH:mm:ss");
        JQModel jqModel = null;
        UserModel userModel = null;
        if (model instanceof JQModel) {
            jqModel = (JQModel) model;
            try {
                String sql = "replace into " + tableName + "(" + "user_id," + "duration," + "start_time," + "end_time," + "riqi," + "jisuan_riqi" + ") values(?,?,?,?,?,?)";//数据库操作语句（插入）
                con = MySQLUtil.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, jqModel.getUserid());
                pst.setString(2, jqModel.getTime());
                pst.setString(3, jqModel.getStarttime());
                pst.setString(4, jqModel.getEndtime());
                pst.setString(5, system_riqi);
                pst.setString(6, jisuan_riqi);
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
        } else if (model instanceof UserModel) {
            userModel = (UserModel) model;
            String sql = "replace into " + tableName + "(" + "user_id," + "unionid," + "openId," + "mobile," + "department," + "OnDuty," + "OffDuty," + "riqi," + "name," + "jisuan_riqi," + "OnResult," + "OffResult" + ") values(?,?,?,?,?,?,?,?,?,?,?,?)";//数据库操作语句（插入）
            //  String sql1 = "insert ignore into USER (user_id,unionid,openIdmobile,department,OnDuty,OffDuty,riqi,name,jisuan_riqi,OnResult,OffResult) values(?,?,?,?,?,?,?,?,?,?,?,?)";//数据库操作语句（插入）
            try {
                con = MySQLUtil.getConnection(props);
                PreparedStatement pst = con.prepareStatement(sql);//用来执行SQL语句查询，对sql语句进行预编译处理
                pst.setString(1, userModel.getUserid());
                pst.setString(2, userModel.getUnionid());
                pst.setString(3, userModel.getOpenId());
                pst.setString(4, userModel.getMobile());
                pst.setString(5, userModel.getDepartment());
                pst.setString(6, userModel.getOnDuty());
                pst.setString(7, userModel.getOffDuty());
                pst.setString(8, system_riqi);
                pst.setString(9, userModel.getName());
                pst.setString(10, jisuan_riqi);
                pst.setString(11, userModel.getOnResult());
                pst.setString(12, userModel.getOffResult());
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
    }
}

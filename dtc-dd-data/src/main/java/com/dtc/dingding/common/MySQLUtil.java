package com.dtc.dingding.common;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class MySQLUtil {


    public static Connection getConnection(Properties props) {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = props.get(PropertiesConstants.MYSQL_USERNAME).toString().trim();
        String passWord = props.get(PropertiesConstants.MYSQL_PASSWORD).toString().trim();
        String host = props.get(PropertiesConstants.MYSQL_HOST).toString().trim();
        String port = props.get(PropertiesConstants.MYSQL_PORT).toString().trim();
        String database = props.get(PropertiesConstants.MYSQL_DATABASE).toString().trim();
        String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl, userName, passWord);//写入mysql数据库
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            System.out.println("数据库连接问题！");
        }
        return connection;
    }
}

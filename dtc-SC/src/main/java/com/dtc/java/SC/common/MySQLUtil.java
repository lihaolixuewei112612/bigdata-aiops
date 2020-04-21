package com.dtc.java.SC.common;

import com.google.common.base.Throwables;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class MySQLUtil {


    public static Connection getConnection(ParameterTool parameterTool) {
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        Connection connection = null;
        try {
            Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String passWord = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl, userName, passWord);//写入mysql数据库
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {

        }

        return connection;
    }
}

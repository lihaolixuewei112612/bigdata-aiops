package com.dtc.java.analytic.V1.sink.mysql;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */
public class MysqlSinkConfigConstants {
    public static final String DRIVERCLASS = "driverClass";
    public static final String DBURL = "dbUrl";
    public static final String USERNAME = "userName";
    public static final String PASSWORD = "passWord";
    public static final String SQL = "sql";

    private static String driverClass ="com.mysql.jdbc.Driver";
    private static String dbUrl="jdbc:mysql://localhost:3306/flink";
    private static String userName ="root";
    private static String passWord ="123456";
    private static String sql="insert";

    public static String getSql() {
        return sql;
    }

    public static String getDriverClass() {
        return driverClass;
    }

    public static String getDbUrl() {
        return dbUrl;
    }

    public static String getUserName() {
        return userName;
    }

    public static String getPassWord() {
        return passWord;
    }
}

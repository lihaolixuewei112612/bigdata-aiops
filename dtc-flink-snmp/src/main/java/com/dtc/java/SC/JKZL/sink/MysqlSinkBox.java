package com.dtc.java.SC.JKZL.sink;

import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.SC.JKZL.model.BoxResultModel;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSinkBox extends RichSinkFunction<BoxResultModel> {
    private Properties properties;
    private Connection connection;
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private PreparedStatement preparedStatement;
    String str;

    public MysqlSinkBox(Properties prop) {
        this.properties = prop;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = properties.get(PropertiesConstants.MYSQL_USERNAME).toString();
        String passWord = properties.get(PropertiesConstants.MYSQL_PASSWORD).toString();
        String host = properties.get(PropertiesConstants.MYSQL_HOST).toString();
        String port = properties.get(PropertiesConstants.MYSQL_PORT).toString();
        String database = properties.get(PropertiesConstants.MYSQL_DATABASE).toString();

        String mysqlUrl = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl, userName
                , passWord);//写入mysql数据库
//        String sql = null;
        String sql = "replace into jigui(riqi,room,partitions,box,allNum,zcNum,wgbgiNum,record,js_time) values(?,?,?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        //insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(BoxResultModel value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            String room = value.getRoom();
            String partitions = value.getPartitions();
            String box = value.getBox();
            double allNum = value.getAllNum();
            double zcNum = value.getZcNum();
            double wgbgjNum = value.getWgbgjNum();
            double record = value.getRecord();
            preparedStatement.setString(1, riqi);
            preparedStatement.setString(2, room);
            preparedStatement.setString(3,partitions);
            preparedStatement.setString(4,box);
            preparedStatement.setInt(5, (int) allNum);
            preparedStatement.setInt(6, (int) zcNum);
            preparedStatement.setInt(7, (int) wgbgjNum);
            preparedStatement.setDouble(8,record);
            preparedStatement.setString(9,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


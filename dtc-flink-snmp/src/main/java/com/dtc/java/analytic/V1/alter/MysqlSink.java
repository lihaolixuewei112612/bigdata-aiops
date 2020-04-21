package com.dtc.java.analytic.V1.alter;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink extends RichSinkFunction<Tuple5<String,String,String,String,String>> {
    private Properties properties;
    private Connection connection;
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private PreparedStatement preparedStatement;
    public MysqlSink(Properties prop){
        this.properties = prop;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(JDBC_DRIVER);
        // 获取数据库连接
        String userName = properties.get(com.dtc.java.analytic.V1.common.constant.PropertiesConstants.MYSQL_USERNAME).toString();
        String passWord = properties.get(com.dtc.java.analytic.V1.common.constant.PropertiesConstants.MYSQL_PASSWORD).toString();
        String host = properties.get(com.dtc.java.analytic.V1.common.constant.PropertiesConstants.MYSQL_HOST).toString();
        String port = properties.get(com.dtc.java.analytic.V1.common.constant.PropertiesConstants.MYSQL_PORT).toString();
        String database = properties.get(com.dtc.java.analytic.V1.common.constant.PropertiesConstants.MYSQL_DATABASE).toString();

        String mysqlUrl= "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = DriverManager.getConnection(mysqlUrl,userName
               ,passWord);//写入mysql数据库
        preparedStatement = connection.prepareStatement(properties.get(PropertiesConstants.SQL).toString());//insert sql在配置文件中
        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }

    @Override
    public void invoke(Tuple5<String,String,String,String,String> value, Context context) throws Exception {
        try {
            String code = value.f0;//获取JdbcReader发送过来的结果
            String host = value.f1;
            String riqi =value.f2;
            String result = value.f3;
            String yuzhi =value.f4;
            preparedStatement.setString(1,code);
            preparedStatement.setString(2,host);
            preparedStatement.setString(3,riqi);
            preparedStatement.setString(4,result);
            preparedStatement.setString(5,yuzhi);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}


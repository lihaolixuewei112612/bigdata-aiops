package com.dtc.java.analytic.V1.sink.mysql;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink extends RichSinkFunction<Tuple2<String,String>> {
    private Properties properties;
    private Connection connection;
    private PreparedStatement preparedStatement;
    public MysqlSink(Properties prop){
        this.properties = prop;
    }
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        Class.forName(properties.get(PropertiesConstants.DRIVER_NAME).toString());
        // 获取数据库连接
        connection = DriverManager.getConnection(properties.get(PropertiesConstants.JDBC_URL).toString(),
                properties.get(PropertiesConstants.USERNAME).toString(),
                properties.get(PropertiesConstants.PASSWORD).toString());//写入mysql数据库
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
    public void invoke(Tuple2<String,String> value, Context context) throws Exception {
        try {
            String name = value._1;//获取JdbcReader发送过来的结果
            preparedStatement.setString(1,name);
            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}


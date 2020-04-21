package com.dtc.java.SC.JSC.sink;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;

/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSinkJSC_TOP extends RichSinkFunction<Tuple5<String, String, Integer, Integer, Double>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        // 加载JDBC驱动
        connection = MySQLUtil.getConnection(parameterTool);
        String sql = "replace into SC_JSC_JKDP_CSTOP(riqi,code,cs_name,alarm_num,cs_sb_num,cs_alarm_rato,js_time) values(?,?,?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sql);

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
    public void invoke(Tuple5<String, String, Integer, Integer, Double> value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());
        //(id,厂商名字，厂商告警数，厂商设备总数，比值)
        try {
            preparedStatement.setString(1, riqi);
            preparedStatement.setString(2,value.f0);
            preparedStatement.setString(3,value.f1);
            preparedStatement.setInt(4,value.f2);
            preparedStatement.setInt(5,value.f3);
            preparedStatement.setDouble(6,value.f4);
            preparedStatement.setString(7,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


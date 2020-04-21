package com.dtc.java.SC.JSC.sink;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.tuple.Tuple9;
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

public class MysqlSinkJSC extends RichSinkFunction<Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = MySQLUtil.getConnection(parameterTool);
        String sql = "replace into SC_JSC_JKDP(riqi,level_1,level_2,level_3,level_4,alarm_count,not_close_alarm,SB_ALL_count,SB_ZC_count,SB_YC_count,js_time) values(?,?,?,?,?,?,?,?,?,?,?)";
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
    public void invoke(Tuple9<Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            preparedStatement.setString(1, riqi);
            preparedStatement.setInt(2,value.f0);
            preparedStatement.setInt(3,value.f1);
            preparedStatement.setInt(4,value.f2);
            preparedStatement.setInt(5,value.f3);
            preparedStatement.setInt(6,value.f4);
            preparedStatement.setInt(7,value.f5);
            preparedStatement.setInt(8,value.f6);
            preparedStatement.setInt(9,value.f7);
            preparedStatement.setInt(10,value.f8);
            preparedStatement.setString(11,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


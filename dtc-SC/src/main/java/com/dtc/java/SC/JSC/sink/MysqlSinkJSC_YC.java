package com.dtc.java.SC.JSC.sink;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.tuple.Tuple8;
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

public class MysqlSinkJSC_YC extends RichSinkFunction<Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double>> {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = MySQLUtil.getConnection(parameterTool);
        String sql = "replace into SC_JSC_JKDP_SBFL(riqi,sb_name,yc_num,sb_all_num,sb_alarm_rato,sb_num_all,sb_num_used,sb_yc_num,sb_yc_rato,js_time) values(?,?,?,?,?,?,?,?,?,?)";
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
    public void invoke(Tuple8<String, Integer, Integer, Double, Integer, Integer, Integer, Double> value, Context context) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String riqi = sdf.format(System.currentTimeMillis());
        String js_time = sdf1.format(System.currentTimeMillis());

        try {
            preparedStatement.setString(1, riqi);
            preparedStatement.setString(2,value.f0);
            preparedStatement.setInt(3,value.f1);
            preparedStatement.setInt(4,value.f2);
            preparedStatement.setDouble(5,value.f3);
            preparedStatement.setInt(6,value.f4);
            preparedStatement.setInt(7,value.f5);
            preparedStatement.setInt(8,value.f6);
            preparedStatement.setDouble(9,value.f7);
            preparedStatement.setString(10,js_time);
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


package com.dtc.java.analytic.V2.sink.mysql;

import com.dtc.java.analytic.V2.common.constant.MySQLUtil;
import com.dtc.java.analytic.V2.common.constant.PropertiesConstants;
import com.dtc.java.analytic.V2.common.model.AlterStruct;
import com.dtc.java.analytic.V2.worker.StreamToFlinkV3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;


/**
 * Created on 2019-09-12
 *
 * @author :hao.li
 */

public class MysqlSink extends RichSinkFunction<AlterStruct> {
    private static final Logger logger = LoggerFactory.getLogger(MysqlSink.class);
    private Connection connection;
    private PreparedStatement preparedStatement;
    private ParameterTool parameterTool;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        // 加载JDBC驱动
        connection = MySQLUtil.getConnection(parameterTool);
        // 加载JDBC驱动
        preparedStatement = connection.prepareStatement(parameterTool.get(PropertiesConstants.SQL));//insert sql在配置文件中
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
    public void invoke(AlterStruct value, Context context) throws Exception {
        try {
            //code,name,asset_id,indice_val,level_id,description,time_occur,rule)
            String code = UUIDGenerator.generateUserCode();
            String code_name = value.getSystem_name();
            String nameCN = value.getNameCN();
            String Unique_id = value.getUnique_id();
            boolean contains = Unique_id.contains("|");
            String asset_id = null;
            String index_id = null;
            String strategy_id = null;
            if (contains) {
                String[] split = Unique_id.split("\\|");
                asset_id = split[0];
                index_id = split[1];
                strategy_id = split[2];
            }
            String real_value = value.getValue();
            String alarm_garde = value.getLevel();
            String description = code_name + "的" + nameCN + "是" + real_value;
            String event_time = value.getEvent_time();
            String alarm_threshold = value.getYuzhi();
            String rule = nameCN + "是" + real_value + ",而阈值是:" + alarm_threshold;
            String s = timeStamp2Date(event_time, "");
            preparedStatement.setString(1, code);
            preparedStatement.setString(2, nameCN);
            preparedStatement.setString(3, asset_id);
            preparedStatement.setString(4, real_value);
            preparedStatement.setString(5, alarm_garde);
            preparedStatement.setString(6, description);
            preparedStatement.setString(7, s);
            preparedStatement.setString(8, rule);
            preparedStatement.setString(9, index_id);
            preparedStatement.setString(10,strategy_id);
            preparedStatement.executeUpdate();
            System.out.println(description+" 打印告警数据写入到msyql中,策略id是 "+ strategy_id);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }
}

class UUIDGenerator {

    private static final String ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final Random rng = new SecureRandom();

    private static char randomChar() {
        return ALPHABET.charAt(rng.nextInt(ALPHABET.length()));
    }

    public static String uuid(int length, int spacing, char spacerChar) {
        StringBuilder sb = new StringBuilder();
        int spacer = 0;
        while (length > 0) {
            if (spacer == spacing) {
                sb.append(spacerChar);
                spacer = 0;
            }
            length--;
            spacer++;
            sb.append(randomChar());
        }
        return sb.toString();
    }

    public static String generateUserCode() {
        return uuid(6, 10, ' ');
    }

//    public static void main(String[] args) {
//        System.out.println(generateUserCode());
//    }
}


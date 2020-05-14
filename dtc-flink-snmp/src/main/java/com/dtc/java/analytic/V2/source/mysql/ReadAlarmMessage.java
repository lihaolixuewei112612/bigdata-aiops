package com.dtc.java.analytic.V2.source.mysql;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created on 2019-12-30
 *
 * @author :hao.li
 */
@Slf4j
public class ReadAlarmMessage extends RichSourceFunction<Tuple9<String, String, String, String, Double, String, String, String, String>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        String driver = "com.mysql.jdbc.Driver";
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST).trim();
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT).trim();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE).trim();
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME).trim();
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD).trim();
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
//            String sql = "select b.asset_id,e.ipv4,c.strategy_id,c.trigger_kind,c.trigger_name,c.number,c.unit,c.`code`,d.is_enable,d.alarm_level,d.up_time,e.`code` as asset_code,e.`name` from (select * from strategy_trigger a \n" +
//                    "where a.code!=\"\" and a.comparator='>') c left join strategy_asset_mapping b on c.strategy_id = b. strategy_id left join alarm_strategy d on c.strategy_id = d.id left join asset e on e.id = b.asset_id";
            String sql = "select o.asset_id,o.ipv4,o.strategy_id,o.trigger_kind,o.trigger_name,o.number,o.unit,o.`code`,o.is_enable,o.alarm_level,o.up_time,o.asset_code,o.`name`,p.id from (select b.asset_id,e.ipv4,c.strategy_id,c.trigger_kind,c.trigger_name,c.number,c.unit,c.`code`,d.is_enable,d.alarm_level,d.up_time,e.`code` as asset_code,e.`name` from (select * from strategy_trigger a \n" +
                    "where a.code!=\"\" and a.comparator='>') c left join strategy_asset_mapping b on c.strategy_id = b. strategy_id left join alarm_strategy d on c.strategy_id = d.id left join asset e on e.id = b.asset_id) o left join asset_indice p on o.code = p.code";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple9<String, String, String, String, Double, String, String, String, String>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                if ("1".equals(resultSet.getString("is_enable"))) {
                    String asset_id = resultSet.getString("asset_id") + "|" + resultSet.getString("id") + "|" + resultSet.getString("strategy_id");
                    String ipv4 = resultSet.getString("ipv4");
                    String strategy_kind = resultSet.getString("trigger_kind");
                    String triger_name = resultSet.getString("trigger_name");
                    double number = resultSet.getDouble("number");
                    String code = resultSet.getString("code");
                    String alarm_level = resultSet.getString("alarm_level");
                    String asset_code = resultSet.getString("asset_code");
                    String name = resultSet.getString("name");
                    ctx.collect(Tuple9.of(asset_id, ipv4, strategy_kind, triger_name, number, code, alarm_level, asset_code, name));
                }
            }
            Thread.sleep(100 * 6);
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            log.error("runException:{}", e);
        }
        isRunning = false;
    }
}

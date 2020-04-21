package com.dtc.java.SC.JKZL;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 未关闭告警设备类型分布情况
 */
@Slf4j
public class ReadDataQY_YCSBFL extends RichSourceFunction<Map<String,Integer>> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String alarm_rule_table = parameterTool.get(PropertiesConstants.MYSQL_ALAEM_TABLE);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        connection = MySQLUtil.getConnection(driver, url, username, password);

        if (connection != null) {
           String sql =  "select m.room,m.zc_name,count(*) as num from (select b.room as room,a.asset_id as a_id,c.`name` as zc_name from asset_category_mapping a left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id) m where m.a_id not in (select DISTINCT asset_id from alarm b where b.`status`=2) GROUP BY m.room,m.zc_name having m.room is not null and m.zc_name is not null";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Map<String, Integer>> ctx) throws Exception {
        Map<String,Integer> map = new HashMap<>();
        Tuple4<String, String, Short, String> test = null;
        Integer id =0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String room = resultSet.getString("room").trim();
                String partitions = resultSet.getString("zc_name").trim();
                id = resultSet.getInt("num");
                map.put(room+"_"+partitions, id);
            }
            log.info("=======select alarm notify from mysql, size = {}, map = {}", map.size(), map);
            ctx.collect(map);
            map.clear();
            Thread.sleep(1000*6);
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

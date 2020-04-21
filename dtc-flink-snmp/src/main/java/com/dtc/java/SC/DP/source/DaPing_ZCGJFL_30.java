package com.dtc.java.SC.DP.source;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 数据大盘-30天内告警等级分类趋势
 */
@Slf4j
public class DaPing_ZCGJFL_30 extends RichSourceFunction<Tuple3<String,String,Integer>> {

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
//            String sql = "select count(*) as AllNum from asset a where a.room is not null and a.partitions is not null and a.box is not null";
            String sql = "select n.`name`,n.level_id,count(*) as AllNum from (select * from (select m.zc_name,m.level_id,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name,alarm.level_id from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id left join asset_category c on c.id = a.asset_category_id left join alarm on alarm.asset_id= a.asset_id) m where m.a_id not in (select DISTINCT asset_id \n" +
                    "from alarm b where b.`status`=2 ) GROUP BY m.zc_name,m.level_id having m.level_id!=\"\") x left join asset_category y on x.pd = y.id) n group by n.`name`,n.level_id ";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String,String,Integer>> ctx) throws Exception {
        Tuple4<String, String, Short, String> test = null;
        int num = 0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String level_id = resultSet.getString("level_id");
                num = resultSet.getInt("AllNum");
                ctx.collect(Tuple3.of(name,level_id,num));
            }
            Thread.sleep(interval_time);
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

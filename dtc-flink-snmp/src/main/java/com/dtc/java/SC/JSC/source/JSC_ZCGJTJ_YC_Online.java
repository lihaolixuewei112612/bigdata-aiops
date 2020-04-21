package com.dtc.java.SC.JSC.source;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 驾驶舱监控大盘--资产类型告警统计
 */
@Slf4j
public class JSC_ZCGJTJ_YC_Online extends RichSourceFunction<Tuple2<String,Integer>> {

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
            String sql = "select z.`name`,sum(num) as num from (select * from (select m.zc_name,m.parent_id as pd,count(*) as num from (select a.asset_id as a_id,c.parent_id,c.`name` as zc_name from asset_category_mapping a \n" +
                    "left join asset b on a.asset_id=b.id and b.`status`=0 left join asset_category c on c.id = a.asset_category_id) m where m.a_id not in (select DISTINCT asset_id \n" +
                    "from alarm b where b.`status`=2) GROUP BY m.zc_name) x left join asset_category y on x.pd = y.id) z group by z.`name`";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple2<String,Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String zc_name = resultSet.getString("name");
                int num = resultSet.getInt("num");
                ctx.collect(Tuple2.of(zc_name,num));
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

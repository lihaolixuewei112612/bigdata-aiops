package com.dtc.java.SC.JKZL;


import com.dtc.java.analytic.V1.alter.MySQLUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import com.dtc.java.SC.JKZL.model.YCShu;
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
 * @Description : 各机房各区域设备正常与不正常数
 */
@Slf4j
public class ReadDataZC_BZC extends RichSourceFunction<YCShu> {

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
            String sql = "select m.room,m.partitions,m.box,n.num as zc,(m.num-n.num) as bzc from (select a.room as room ,a.partitions as partitions,a.box as box,count(*) as num from asset a group by a.room,a.partitions ,a.box having a.room is not null and a.partitions is not null and a.box is not null) m left join (select b.room as room,b.partitions as partitions,b.box as box,count(*) as num from asset b where b.id not in (select distinct asset_id from alarm) group by b.room,b.partitions,b.box having b.room is not null and b.partitions is not null and b.box is not null) n on m.room=n.room and m.partitions=n.partitions and m.box=n.box";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<YCShu> ctx) throws Exception {
        Map<String, String> map = new HashMap<>();
        YCShu ycshu = null;
        Tuple4<String, String, Short, String> test = null;
        Integer id = 0;
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String room = resultSet.getString("room").trim();
                String partitions = resultSet.getString("partitions").trim();
                String box = resultSet.getString("box");
                double zc = resultSet.getInt("zc");
                double bzc = resultSet.getInt("bzc");
                Long time = System.currentTimeMillis();
                ycshu = new YCShu(room,partitions,box,zc,bzc);
                ctx.collect(ycshu);
            }
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

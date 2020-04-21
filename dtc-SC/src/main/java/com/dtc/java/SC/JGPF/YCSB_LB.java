package com.dtc.java.SC.JGPF;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.JKZL.model.YCSB_LB_Model;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 各机房未关闭告警数
 */
@Slf4j
public class YCSB_LB extends RichSourceFunction<YCSB_LB_Model> {

    private Connection connection = null;
    private PreparedStatement ps = null;
    private volatile boolean isRunning = true;
    private ParameterTool parameterTool;
    private long interval_time;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        interval_time = Long.parseLong(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        connection = MySQLUtil.getConnection(parameterTool);

        if (connection != null) {
           String sql = "select a.asset_id,a.level_id,count(*) as num,b.`name`,b.ipv4 as ip,b.room,b.partitions,b.box from alarm a left join asset b on b.id =a.asset_id group by a.asset_id,a.level_id having b.room is not null and b.`partitions` is not null and b.box is not null";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<YCSB_LB_Model> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String asset_id = resultSet.getString("asset_id").trim();
                String level_id = resultSet.getString("level_id").trim();
                String name = resultSet.getString("name").trim();
                String ip = resultSet.getString("ip").trim();
                int num = resultSet.getInt("num");
                String room = resultSet.getString("room");
                String partitions = resultSet.getString("partitions");
                String box = resultSet.getString("box");
                YCSB_LB_Model ycsb_lb_model = new YCSB_LB_Model(asset_id,level_id,num,name,ip,room,partitions,box);
                ctx.collect(ycsb_lb_model);
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

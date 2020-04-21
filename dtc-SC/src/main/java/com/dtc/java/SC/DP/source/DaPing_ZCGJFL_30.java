package com.dtc.java.SC.DP.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
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
    private long interval_time;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        interval_time = Long.parseLong(parameterTool.get(PropertiesConstants.INTERVAL_TIME));
        connection = MySQLUtil.getConnection(parameterTool);

        if (connection != null) {
//            String sql = "select ifnull(n.p_name,'其他') as `name`,\n" +
//                    "case \n" +
//                    "when level_id=1 then '一般'\n" +
//                    "when level_id=2 then '较严重'\n" +
//                    "when level_id=3 then '严重'\n" +
//                    "when level_id=4 then '灾难'\n" +
//                    "end as level_id,\n" +
//                    "count(*) as AllNum from (select m.asset_id,m.pd,m.`name` as z_name,m.level_id,y.id,y.parent_id,y.`name` as p_name from (select a.asset_id,c.parent_id as pd,c.`name`,alarm.level_id from asset_category_mapping a left join asset b on a.asset_id=b.id \n" +
//                    "left join asset_category c on c.id = a.asset_category_id left join alarm on alarm.asset_id= a.asset_id) m left join asset_category y on m.pd=y.id) n group by n.p_name,n.level_id having n.level_id!=\"\"";
//生产环境下使用
//            String sql = "select ifnull(n.p_name,'其他') as `name`,\n" +
//                    "case \n" +
//                    "when level_id=1 then '一般'\n" +
//                    "when level_id=2 then '较严重'\n" +
//                    "when level_id=3 then '严重'\n" +
//                    "when level_id=4 then '灾难'\n" +
//                    "end as level_id,\n" +
//                    "count(*) as AllNum from (select m.asset_id,m.pd,m.`name` as z_name,m.level_id,y.id,y.parent_id,y.`name` as p_name from (select a.asset_id,c.parent_id as pd,c.`name`,alarm.level_id from asset_category_mapping a left join asset b on a.asset_id=b.id \n" +
//                    "left join asset_category c on c.id = a.asset_category_id left join alarm on alarm.asset_id= a.asset_id and TO_DAYS(alarm.time_occur) = TO_DAYS(NOW())) m left join asset_category y on m.pd=y.id) n group by n.p_name,n.level_id having n.level_id!=\"\"";

            String sql = "select ifnull(n.p_name,'其他') as `name`,n.level_id,count(*) as AllNum from (select m.asset_id,m.pd,m.`name` as z_name,m.level_id,y.id,y.parent_id,y.`name` as p_name from (select a.asset_id,c.parent_id as pd,c.`name`,alarm.level_id from asset_category_mapping a left join asset b on a.asset_id=b.id \n" +
                    "left join asset_category c on c.id = a.asset_category_id left join alarm on alarm.asset_id= a.asset_id and TO_DAYS(alarm.time_occur) = TO_DAYS(NOW())) m left join asset_category y on m.pd=y.id) n group by n.p_name,n.level_id having n.level_id!=\"\"";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String,String,Integer>> ctx) throws Exception {
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

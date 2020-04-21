package com.dtc.java.SC.JSC.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Author : lihao
 * Created on : 2020-03-24
 * @Description : 驾驶舱监控大盘--厂商设备top告警
 */
@Slf4j
public class JSC_CSSB_TOP_GJTJFX extends RichSourceFunction<Tuple5<String,String,Integer,Integer,Integer>> {

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
//            String sql = "select count(*) as AllNum from asset a where a.room is not null and a.partitions is not null and a.box is not null";
            String sql = "select manufacturer_id,`name`,one_num,num from (select c.manufacturer_id,c.num,b.`name` from (select a.manufacturer_id,count(*) as num from asset a GROUP BY a.manufacturer_id having a.manufacturer_id is not null) c left join manufacturer b on  c.manufacturer_id = b.id) x \n" +
                    "right join (select c.manufacturer_id as one_id,c.num as one_num ,b.`name` as one_name from (select a.manufacturer_id,count(*) as num from asset a where a.id in(select distinct asset_id from alarm) GROUP BY a.manufacturer_id having a.manufacturer_id!=\"\") c left join manufacturer b on  c.manufacturer_id = b.id) y on x.manufacturer_id = y.one_id";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple5<String,String,Integer,Integer,Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String manufacturer_id = resultSet.getString("manufacturer_id");
                String name = resultSet.getString("name");
                int one_num = resultSet.getInt("one_num");
                int num = resultSet.getInt("num");
                ctx.collect(Tuple5.of(manufacturer_id,name,one_num,num,1));
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

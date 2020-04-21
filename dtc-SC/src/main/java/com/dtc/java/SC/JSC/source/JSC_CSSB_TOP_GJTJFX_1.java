package com.dtc.java.SC.JSC.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
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
 * @Description : 驾驶舱监控大盘--厂商设备top告警--总告警台数
 */
@Slf4j
public class JSC_CSSB_TOP_GJTJFX_1 extends RichSourceFunction<Tuple2<Integer,Integer>> {

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
            String sql = "select sum(o.one_num) as NUM from (select manufacturer_id,`name`,one_num,num from (select c.manufacturer_id,c.num,b.`name` from (select a.manufacturer_id,count(*) as num \n" +
                    "from asset a GROUP BY a.manufacturer_id having a.manufacturer_id is not null) c left join manufacturer b on  c.manufacturer_id = b.id) x right join \n" +
                    "(select c.manufacturer_id as one_id,c.num as one_num ,b.`name` as one_name from (select a.manufacturer_id,count(*) as num from asset a \n" +
                    "where a.id in(select distinct asset_id from alarm where TO_DAYS(now())=TO_DAYS(time_occur)) GROUP BY a.manufacturer_id having a.manufacturer_id!=\"\") c \n" +
                    "left join manufacturer b on  c.manufacturer_id = b.id) y on x.manufacturer_id = y.one_id) o";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple2<Integer,Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int one_num = resultSet.getInt("NUM");
                ctx.collect(Tuple2.of(one_num,1));
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

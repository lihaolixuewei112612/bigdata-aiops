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
 * @Description : 驾驶舱-监控大盘-未关闭告警数
 */
@Slf4j
public class JSC_WGBGJ extends RichSourceFunction<Tuple2<Integer, Integer>> {

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
            String sql = "select count(*) as num from asset a where a.id not in (select distinct asset_id from alarm b where b.`status`=2 and TO_DAYS(b.time_occur) = TO_DAYS(NOW()))";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int num = resultSet.getInt("num");
                ctx.collect(Tuple2.of(1, num));
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

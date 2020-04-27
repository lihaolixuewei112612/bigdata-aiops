package com.dtc.java.SC.JSC.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple7;
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
public class JSC_GL_SOURCE_2 extends RichSourceFunction<Tuple7<String,String,String,String,String,String,String>> {

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
            String sql = "SELECT\n" +
                    "CAST(ROUND((sjgd/zs*100),2) AS CHAR) as sjgd,\n" +
                    "CAST(ROUND((bgsx/zs*100),2) AS CHAR) as bgsx,\n" +
                    "CAST(ROUND((fwgd/zs*100),2) AS CHAR) as fwgd,\n" +
                    "CAST(ROUND((gjgd/zs*100),2) AS CHAR) as gjgd,\n" +
                    "CAST(ROUND((zysq/zs*100),2) AS CHAR) as zysq,\n" +
                    "CAST(ROUND((wpgd/zs*100),2) AS CHAR) as wpgd,\n" +
                    "CAST(ROUND((qt/zs*100),2) AS CHAR) as qt\n" +
                    "FROM\n" +
                    "(SELECT\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='1' GROUP BY a.type) AS sjgd,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='2' GROUP BY a.type) AS bgsx,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='3' GROUP BY a.type) AS fwgd,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='4' GROUP BY a.type) AS gjgd,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='5' GROUP BY a.type) AS zysq,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='6' GROUP BY a.type) AS wpgd,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  WHERE a.type='7' GROUP BY a.type) AS qt,\n" +
                    "(SELECT COUNT(*) AS numb FROM work_order a  )AS zs\n" +
                    ") as T\n";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple7<String,String,String,String,String,String,String>> ctx) throws Exception {
        while (isRunning) {
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                String sjgd = rsq.getString("sjgd");
                String bgsx = rsq.getString("bgsx");
                String fwgd = rsq.getString("fwgd");
                String gjgd = rsq.getString("gjgd");
                String zysq = rsq.getString("zysq");
                String wpgd = rsq.getString("wpgd");
                String qt = rsq.getString("qt");
                ctx.collect(Tuple7.of(sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt));
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

package com.dtc.java.SC.JSC.source;


import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple13;
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
public class JSC_GL_SOURCE_1 extends RichSourceFunction<Tuple13<String,String,String,String,String,String,String,String,String,String,String,String,String>> {

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
            String sql = "SELECT IFNULL(wclgd,0) AS wclgd,IFNULL(jrpd,0) AS jrpd,IFNULL(jrdk,0) AS jrdk,IFNULL(zjbcs,0) AS zjbcs,IFNULL(zjbrs,0) AS zjbrs,IFNULL(zrs,0) AS  zrs,IFNULL(jrjbcs,0) AS jrjbcs,\n" +
                    "IFNULL((jrdk-jrjbcs),0) as jrwjb,IFNULL(yjbcs,0) AS yjbcs,IFNULL(gjgd,0) AS gjgd,IFNULL((tgjgd-gjgd),0) AS tbgjgd,IFNULL(swgd,0) AS swgd,IFNULL((tswgd-swgd),0) as tbswgd\n" +
                    "FROM (\n" +
                    "SELECT (SELECT COUNT(state)from work_order WHERE work_order.state='1') as wclgd,\n" +
                    "(SELECT COUNT(state) as jr FROM work_order WHERE TO_DAYS(predict_start_time)=TO_DAYS(now())) AS jrpd,\n" +
                    "(SELECT count(*) FROM SC_KQ_USER WHERE TO_DAYS(jisuan_riqi)=TO_DAYS(DATE_FORMAT(NOW(),'%Y%m%d')) and OnDuty!=\"null\") AS jrdk,\n" +
                    "(SELECT count(*) AS zjbrcs  FROM SC_KQ WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 7 DAY) <= date(k_jisuan_riqi) AND j_duration!='null') AS zjbcs,\n" +
                    "(SELECT count(j_duration)AS zjbrcs  FROM SC_KQ WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 30 DAY) <= date(k_jisuan_riqi)AND OnResult!='null' AND j_duration!='null') AS yjbcs,\n" +
                    "(SELECT COUNT(DISTINCT (k_name)) AS zjbrs  FROM SC_KQ WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 7 DAY) <= date(k_jisuan_riqi)AND OnResult!='null' AND j_duration!='null') AS zjbrs,\n" +
                    "(SELECT COUNT(DISTINCT user_id)FROM SC_KQ_USER) AS zrs,\n" +
                    "(SELECT count(j_duration) AS zjbrcs  FROM SC_KQ  WHERE TO_DAYS(k_jisuan_riqi) = TO_DAYS(NOW())AND OnResult!='null' AND j_duration!='null') AS jrjbcs,\n" +
                    "(SELECT COUNT(code)as zgd from work_order) AS zgd,\n" +
                    "(SELECT COUNT(`code`)as zgj FROM alarm)  AS zgj,\n" +
                    "(SELECT COUNT(a.`status`)as gjdcl FROM alarm AS a WHERE a.`status`='0')AS gjdcl,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 7 DAY) <= date(create_time)AND work_order.type='4') AS gjgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 14 DAY) <= date(create_time)AND work_order.type='4') AS tgjgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 7 DAY) <= date(create_time)AND work_order.type='1') AS swgd,\n" +
                    "(SELECT count(NAME) AS gjgd FROM work_order WHERE DATE_SUB(DATE_FORMAT(CURDATE(),'%Y%m%d'), INTERVAL 14 DAY) <= date(create_time)AND work_order.type='1') AS tswgd\n" +
                    ") as T";//数据库操作语句（查询）
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple13<String,String,String,String,String,String,String,String,String,String,String,String,String>> ctx) throws Exception {


        while (isRunning) {
            ResultSet rsq = ps.executeQuery();
            while (rsq.next()) {
                //未处理工单
                String wclgd = rsq.getString("wclgd");
                //今日派单
                String jrpd = rsq.getString("jrpd");
                //今日打卡人数
                String jrdk = rsq.getString("jrdk");
                //周加班次数
                String zjbcs = rsq.getString("zjbcs");
                //周加班人数
                String zjbrs = rsq.getString("zjbrs");
                //周加班次数
                String jrjbcs = rsq.getString("jrjbcs");
                //今日未加班
                String jrwjb = rsq.getString("jrwjb");
                //月加班次数
                String yjbcs = rsq.getString("yjbcs");
                //告警工单
                String gjgd = rsq.getString("gjgd");
                //同比告警工单
                String tbgjgd = rsq.getString("tbgjgd");
                //事务工单
                String swgd = rsq.getString("swgd");
                //同比事务工单
                String tbswgd = rsq.getString("tbswgd");
                //总人数
                String zrs = rsq.getString("zrs");
                ctx.collect(Tuple13.of(wclgd,jrpd,jrdk,zjbcs,zjbrs,jrjbcs,jrwjb,yjbcs,gjgd,tbgjgd,swgd,tbswgd,zrs));
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

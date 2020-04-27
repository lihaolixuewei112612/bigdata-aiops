package com.dtc.java.SC.JSC.source;
import com.dtc.java.SC.common.MySQLUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * @Description : 驾驶舱监控大盘--资产类型告警统计--异常资产总数
 */
@Slf4j
public class JSC_ZCGJTJ_YC extends RichSourceFunction<Tuple3<String,Integer,Integer>> {

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
            String sql = "select ifnull(k.name,'其他') as name,ifnull(l.num,0) as num from asset_category k left join(select j.name,f.num\n" +
                    "from (select d.asset_id,d.num,d.asset_category_id,e.parent_id\n" +
                    "from (select b.asset_id,b.num,c.asset_category_id from (select a.asset_id,count(*) as num from alarm a where TO_DAYS(now())=to_days(a.time_occur) group by a.asset_id) b \n" +
                    "left join asset_category_mapping c on c.asset_id=b.asset_id) d left join asset_category e on e.id = d.asset_category_id) f left join asset_category j on j.id=f.parent_id) l on k.name = l.name where k.parent_id=0";
            ps = connection.prepareStatement(sql);
        }
    }

    @Override
    public void run(SourceContext<Tuple3<String,Integer,Integer>> ctx) throws Exception {
        while (isRunning) {
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String zc_name = resultSet.getString("name").trim();
                int num = resultSet.getInt("num");
                ctx.collect(Tuple3.of(zc_name,num,1));
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

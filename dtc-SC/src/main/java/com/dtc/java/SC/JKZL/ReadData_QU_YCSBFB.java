package com.dtc.java.SC.JKZL;//package com.dtc.java.shucang.JFSBWGBGJ;
//
//
//import com.dtc.java.analytic.V1.alter.MySQLUtil;
//import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
//import com.dtc.java.shucang.JFSBWGBGJ.model.FBModel;
//import com.dtc.java.shucang.JFSBWGBGJ.model.ZongShu;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.flink.api.java.tuple.Tuple4;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.util.HashMap;
//import java.util.Map;
//
///**
// * @Author : lihao
// * Created on : 2020-03-24
// * @Description : 各机房各区域异常设备类型分布情况
// */
//@Slf4j
//public class ReadData_QU_YCSBFB extends RichSourceFunction<ZongShu> {
//
//    private Connection connection = null;
//    private PreparedStatement ps = null;
//    private volatile boolean isRunning = true;
//    private ParameterTool parameterTool;
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
//        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
//        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
//        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
//        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
//        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
//        String alarm_rule_table = parameterTool.get(PropertiesConstants.MYSQL_ALAEM_TABLE);
//
//        String driver = "com.mysql.jdbc.Driver";
//        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
//        connection = MySQLUtil.getConnection(driver, url, username, password);
//
//        if (connection != null) {
//           String sql =  "select m.room,m.partitions,m.system,m.num/sum(m.num) as precent from (select a.room,a.partitions,a.system as system,count(*) as num from asset a where a.id in(select distinct asset_id from alarm) group by a.system,a.room,a.partitions) m";
//            ps = connection.prepareStatement(sql);
//        }
//    }
//
//    @Override
//    public void run(SourceContext<ZongShu> ctx) throws Exception {
//        Map<String,Integer> map = new HashMap<>();
//        Tuple4<String, String, Short, String> DPComplete = null;
//        ZongShu fbModel = null;
//        double id =0;
//        while (isRunning) {
//            ResultSet resultSet = ps.executeQuery();
//            while (resultSet.next()) {
//                String room = resultSet.getString("room");
//                String partitions = resultSet.getString("partitions");
//                String system = resultSet.getString("system");
//                id = resultSet.getInt("precent");
//                fbModel = new ZongShu(room,partitions,system,id);
//                ctx.collect(fbModel);
//            }
//
//            Thread.sleep(1000*60);
//        }
//
//    }
//
//    @Override
//    public void cancel() {
//        try {
//            super.close();
//            if (connection != null) {
//                connection.close();
//            }
//            if (ps != null) {
//                ps.close();
//            }
//        } catch (Exception e) {
//            log.error("runException:{}", e);
//        }
//        isRunning = false;
//    }
//}

package com.dtc.java.SC.WDZL;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/*我的总览 sink
* */
public class WdzlSink extends RichSinkFunction<Map<String, List>> {
    private ParameterTool parameterTool;
    private static Connection connection = null;

    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatement2;
    private PreparedStatement preparedStatement3;
    private PreparedStatement preparedStatement4;
    private PreparedStatement preparedStatement5;
    private PreparedStatement preparedStatement6;
    private PreparedStatement preparedStatement7;

    @Override

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = MySQLUtil.getConnection(parameterTool);
        //insert sql工作动态
        String sinkSql = "replace INTO SC_WDZL_GZDT (`zgd`, `fpgw`, `hd`,`tw`,`zgj`,`gjdcl`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?,?,?);";
        preparedStatement = connection.prepareStatement(sinkSql);
        //insert sql 我的工单
        String sinkSql2 = "replace INTO SC_WDZL_WDGD (`title`, `type`, `name`,`state`,`time`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?,?);";
        preparedStatement2 = connection.prepareStatement(sinkSql2);
        //工作问题
        String sinkSql3 = "replace INTO SC_WDZL_GZWT (`title`, `ans`, `name`,`time`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?);";
        preparedStatement3 = connection.prepareStatement(sinkSql3);
        //知识库分布
        String sinkSql4 = "replace INTO SC_WDZL_ZSKFB (`szjy`,`llzs`,`pxkc`,`riqi`,`js_time`) VALUES (?,?,?,?,?);";
        preparedStatement4 = connection.prepareStatement(sinkSql4);
        //我处理的工单分布 sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt,js
        String sinkSql5 = "replace INTO SC_WDZL_WGLDGDFB (`sjgd`, `bgsx`, `fwgd`,`gjgd`,`zysq`, `wpgd`, `qt`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?,?,?,?);";
        preparedStatement5 = connection.prepareStatement(sinkSql5);
        ////项目工单近6个个月的趋势a,b,c,d,e,f,js
        String sinkSql6 = "replace INTO SC_WDZL_XMGDQS (`a`, `b`, `c`,`d`,`e`, `f`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?,?,?);";
        preparedStatement6 = connection.prepareStatement(sinkSql6);
        //资产资源分布
        String sinkSql7 = "replace INTO SC_WDZL_ZCZYFB (`zhuji`,`wlsb`, `aqsb`, `ccsb`,`jcss`,`riqi`,`js_time`) VALUES (?,?,?,?,?,?,?);";
        preparedStatement7 = connection.prepareStatement(sinkSql7);
    }

    @Override
    public void invoke(Map<String, List> value, Context context) throws Exception {
        String time = String.valueOf(System.currentTimeMillis());
        String riqi = timeStamp2Date(time, "yyyy-MM-dd ");
        String js_time = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        //工作动态
        List listA = value.get("GZDT");
        Map mapA = null;
        for (int i = 0; i < listA.size(); i++) {
            mapA = (Map) listA.get(i);
            preparedStatement.setString(1, String.valueOf(mapA.get("zgd")));
            preparedStatement.setString(2, String.valueOf(mapA.get("fpgw")));
            preparedStatement.setString(3, String.valueOf(mapA.get("hd")));
            preparedStatement.setString(4, String.valueOf(mapA.get("tw")));
            preparedStatement.setString(5, String.valueOf(mapA.get("zgj")));
            preparedStatement.setString(6, String.valueOf(mapA.get("gjdcl")));
            preparedStatement.setString(7, riqi);
            preparedStatement.setString(8, js_time);
            preparedStatement.executeUpdate();
        }
        //我的工单
        List listB = value.get("WDGD");
        Map mapB = null;
        for (int i = 0; i < listB.size(); i++) {
            mapB = (Map) listB.get(i);
            preparedStatement2.setString(1, String.valueOf(mapB.get("title")));
            preparedStatement2.setString(2, String.valueOf(mapB.get("type")));
            preparedStatement2.setString(3, String.valueOf(mapB.get("name")));
            preparedStatement2.setString(4, String.valueOf(mapB.get("state")));
            preparedStatement2.setString(5, String.valueOf(mapB.get("time")));
            preparedStatement2.setString(6, riqi);
            preparedStatement2.setString(7, js_time);
            preparedStatement2.executeUpdate();
        }
        //工作问题
        List listC = value.get("GZWT");
        Map mapC = null;
        for (int i = 0; i < listC.size(); i++) {
            mapC = (Map) listC.get(i);
            preparedStatement3.setString(1, String.valueOf(mapC.get("title")));
            preparedStatement3.setString(2, String.valueOf(mapC.get("ans")));
            preparedStatement3.setString(3, String.valueOf(mapC.get("name")));
            preparedStatement3.setString(4, String.valueOf(mapC.get("time")));
            preparedStatement3.setString(5, riqi);
            preparedStatement3.setString(6, js_time);
            preparedStatement3.executeUpdate();
        }

        //知识库分布
        List listD = value.get("ZSKFB");
        Map mapD = null;
        for (int i = 0; i < listD.size(); i++) {
            mapD = (Map) listD.get(i);
            preparedStatement4.setString(1, String.valueOf(mapD.get("szjy")));
            preparedStatement4.setString(2, String.valueOf(mapD.get("llzs")));
            preparedStatement4.setString(3, String.valueOf(mapD.get("pxkc")));
            preparedStatement4.setString(4, riqi);
            preparedStatement4.setString(5, js_time);
            preparedStatement4.executeUpdate();
        }
        //我处理的工单分布 sjgd,bgsx,fwgd,gjgd,zysq,wpgd,qt,js
        List listE = value.get("WCLDGDFB");
        Map mapE = null;
        for (int i = 0; i < listE.size(); i++) {
            mapE = (Map) listE.get(i);
            preparedStatement5.setString(1, String.valueOf(mapE.get("sjgd")));
            preparedStatement5.setString(2, String.valueOf(mapE.get("bgsx")));
            preparedStatement5.setString(3, String.valueOf(mapE.get("fwgd")));
            preparedStatement5.setString(4, String.valueOf(mapE.get("gjgd")));
            preparedStatement5.setString(5, String.valueOf(mapE.get("zysq")));
            preparedStatement5.setString(6, String.valueOf(mapE.get("wpgd")));
            preparedStatement5.setString(7, String.valueOf(mapE.get("qt")));
            preparedStatement5.setString(8, riqi);
            preparedStatement5.setString(9, js_time);
            preparedStatement5.executeUpdate();
        }
        //项目工单近6个个月的趋势a,b,c,d,e,f,js
        List listF = value.get("WCLDGJ");
        Map mapF = null;
        for (int i = 0; i < listF.size(); i++) {
            mapF = (Map) listF.get(i);
            preparedStatement6.setString(1, String.valueOf(mapF.get("a")));
            preparedStatement6.setString(2, String.valueOf(mapF.get("b")));
            preparedStatement6.setString(3, String.valueOf(mapF.get("c")));
            preparedStatement6.setString(4, String.valueOf(mapF.get("d")));
            preparedStatement6.setString(5, String.valueOf(mapF.get("e")));
            preparedStatement6.setString(6, String.valueOf(mapF.get("f")));
            preparedStatement6.setString(7, riqi);
            preparedStatement6.setString(8, js_time);
            preparedStatement6.executeUpdate();
        }
        //资产资源分布
        List listG = value.get("ZCZYFB");
        Map mapG = null;
        for (int i = 0; i < listG.size(); i++) {
            mapG = (Map) listG.get(i);
            preparedStatement7.setString(1, String.valueOf(mapG.get("zhuji")));
            preparedStatement7.setString(2, String.valueOf(mapG.get("wlsb")));
            preparedStatement7.setString(3, String.valueOf(mapG.get("aqsb")));
            preparedStatement7.setString(4, String.valueOf(mapG.get("ccsb")));
            preparedStatement7.setString(5, String.valueOf(mapG.get("jcss")));
            preparedStatement7.setString(6, riqi);
            preparedStatement7.setString(7, js_time);
            preparedStatement7.executeUpdate();


        }
    }
    @Override
    public void close() throws Exception {
        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (preparedStatement2 != null) {
            preparedStatement2.close();
        }
        if (preparedStatement3 != null) {
            preparedStatement3.close();
        }
        if (preparedStatement4 != null) {
            preparedStatement4.close();
        }
        if (preparedStatement5 != null) {
            preparedStatement5.close();
        }
        if (preparedStatement6 != null) {
            preparedStatement6.close();
        }
        if (preparedStatement7 != null) {
            preparedStatement7.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
    private static String timeStamp2Date(String seconds, String format) {
        if (seconds == null || seconds.isEmpty() || seconds.equals("null")) {
            return "";
        }
        if (format == null || format.isEmpty()) {
            format = "yyyy-MM-dd HH:mm:ss";
        }
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(new Date(Long.valueOf(seconds)));
    }
}

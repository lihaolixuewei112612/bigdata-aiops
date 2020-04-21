package com.dtc.java.SC.JSC.gldp;

import com.dtc.java.SC.common.MySQLUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
/*驾驶舱管理大盘 sink*/
public class Lwrite  extends RichSinkFunction<Map<String, String>> {
    private ParameterTool parameterTool;
    private static Connection connection = null;

    private PreparedStatement preparedStatement;
    private PreparedStatement preparedStatement2;
    private PreparedStatement preparedStatement3;
    @Override

    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 加载JDBC驱动
        parameterTool = (ParameterTool) (getRuntimeContext().getExecutionConfig().getGlobalJobParameters());
        connection = MySQLUtil.getConnection(parameterTool);
        String sinkSql = "replace into SC_JSC_GLDP_GDXX (`gname`, `wcgd`, `gjsl`, `riqi`, `js_time`) VALUES " +
                "(?,?,?,?,?)";
        preparedStatement = connection.prepareStatement(sinkSql);//insert sql在配置文件中
        String sinkSql2 = "replace into SC_JSC_GLDP_BP(`wclgd`,`jrpd`,`jrdk`,`zjbcs`,`zjbrs`,`zrs`,`jrjbcs`,`jrwjb`,`yjbcs`,`gjgd`,`tbgjgd`,`swgd`,`tbswgd`,`riqi`,`js_time`)VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        preparedStatement2 = connection.prepareStatement(sinkSql2);//insert sql在配置文件中
        String sinkSql3 = "replace into SC_JSC_GLDP_ZZFW (`sjgd`, `bgsx`,`fwgd`,`gjgd`,`zysq`,`wpgd`,`qt`,`riqi`,`js_time`) VALUES " +
                "(?,?,?,?,?,?,?,?,?)";
        preparedStatement3 = connection.prepareStatement(sinkSql3);//insert sql在配置文件中
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
        if (connection != null) {
            connection.close();
        }
    }
    @Override
    public void invoke(Map<String, String> value, Context context) throws Exception {
        String time = String.valueOf(System.currentTimeMillis());
        String riqi = timeStamp2Date(time, "yyyy-MM-dd ");
        String js_time = timeStamp2Date(time, "yyyy-MM-dd HH:mm:ss");
        preparedStatement.setString(1, value.get("gname"));
        preparedStatement.setString(2, value.get("wcgd"));
        preparedStatement.setString(3, value.get("gjsl"));
        preparedStatement.setString(4, riqi);
        preparedStatement.setString(5, js_time);
        preparedStatement.executeUpdate();
        preparedStatement2.setString(1, value.get("wclgd"));
        preparedStatement2.setString(2, value.get("jrpd"));
        preparedStatement2.setString(3, value.get("jrdk"));
        preparedStatement2.setString(4, value.get("zjbcs"));
        preparedStatement2.setString(5, value.get("zjbrs"));
        preparedStatement2.setString(6, value.get("zrs"));
        preparedStatement2.setString(7, value.get("jrjbcs"));
        preparedStatement2.setString(8, value.get("jrwjb"));
        preparedStatement2.setString(9, value.get("yjbcs"));
        preparedStatement2.setString(10, value.get("gjgd"));
        preparedStatement2.setString(11, value.get("tbgjgd"));
        preparedStatement2.setString(12, value.get("swgd"));
        preparedStatement2.setString(13, value.get("tbswgd"));
        preparedStatement2.setString(14,riqi);
        preparedStatement2.setString(15, js_time);
        preparedStatement2.executeUpdate();
        preparedStatement3.setString(1, value.get("sjgd"));
        preparedStatement3.setString(2, value.get("bgsx"));
        preparedStatement3.setString(3, value.get("fwgd"));
        preparedStatement3.setString(4, value.get("gjgd"));
        preparedStatement3.setString(5, value.get("zysq"));
        preparedStatement3.setString(6, value.get("wpgd"));
        preparedStatement3.setString(7, value.get("qt"));
        preparedStatement3.setString(8, riqi);
        preparedStatement3.setString(9, js_time);
        preparedStatement3.executeUpdate();

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

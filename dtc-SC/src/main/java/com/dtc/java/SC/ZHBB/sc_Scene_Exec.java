package com.dtc.java.SC.ZHBB;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.SC.common.PropertiesConstants;
import com.dtc.java.SC.ZHBB.common.SC_Scene_ZHBB;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @Author : lihao
 * Created on : 2020-04-15
 * @Description : 综合报表代码
 */
public class sc_Scene_Exec {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        SC_Scene_ZHBB.sc_Scence_one(env,driver,url,username,password);
        SC_Scene_ZHBB.sc_Scence_Two(env,driver,url,username,password);

        env.execute("WordCount Example");

    }

}

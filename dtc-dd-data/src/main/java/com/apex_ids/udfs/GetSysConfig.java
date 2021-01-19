package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author linqz
 */
@Description(
        name = "GetSysConfig",
        value = "_FUNC_(paramKind,paramCode,defaultValue, isCheck) - 获取系统工作参数 "
)
public class GetSysConfig extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetSysConfig.class);


    private static final String SPLIT_FLAG = "_";
    private static final String NUMBER_TYPE = "NUMBER";
    private static final String CHAR_TYPE = "CHAR";

    private static Pattern pattern = Pattern.compile("\\d+");

    private static Map<String, String> dicParamMap = new HashMap<>(32);
    private static Map<String, String> dicParamTypeMap = new HashMap<>(32);
    public static void initCache(){
        Properties props = new Properties();
        Connection conn = null;
        try {
            props.load(GetEtlBit.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            //获取系统工作参数
            conn = DriverManager.getConnection(props.getProperty("pub_sys_url"), props);
            PreparedStatement ps = conn.prepareStatement("select param_kind, param_code,  param_val, param_type from t_sys_param");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                dicParamMap.put(rs.getString("param_kind") + SPLIT_FLAG + rs.getString("param_code"),
                        rs.getString("param_val"));
                dicParamTypeMap.put(rs.getString("param_kind") + SPLIT_FLAG + rs.getString("param_code"),
                        rs.getString("param_type"));
            }
        } catch (Exception e) {
            log.error(e.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {

                }
            }
        }
    }

    static {
        initCache();
    }

    public Text evaluate(Text paramKind, Text paramCode, Text defaultValue, IntWritable isCheck) {
        if (paramKind == null || paramCode == null) {
            return null;
        }
        String val = dicParamMap.get(paramKind.toString() + SPLIT_FLAG + paramCode.toString());
        String type = dicParamTypeMap.get(paramKind.toString() + SPLIT_FLAG + paramCode.toString());
        if (val == null || "".equals(val)) {
            return defaultValue;
        }
        if (isCheck.get() == 1 && NUMBER_TYPE.equals(type)) {
            Matcher matcher = pattern.matcher(val);
            if (!matcher.matches()) {
                return defaultValue;
            }
        }
        return new Text(val);
    }


}

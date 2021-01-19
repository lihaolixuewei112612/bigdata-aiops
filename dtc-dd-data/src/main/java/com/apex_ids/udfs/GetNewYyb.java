package com.apex_ids.udfs;

import com.apex_ids.util.StringUtil;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;


/**
 * @author linqz
 */
@Description(
        name = "GetNewYyb",
        value = "_FUNC_(yyb, tranType) - etl yyb code "
)
public class GetNewYyb extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetNewYyb.class);

    private static final String TO_NUMBER = "TO_NUMBER";
    private static final String PREFIX = "PREFIX";
    private static final String POSTFIX = "POSTFIX";
    private static final String MAPP = "MAPP";
    /**
     * 是否启用营业部清洗
     */
    private static String isTranNewYyb = "";
    /**
     * 营业部清洗前缀
     */
    private static String tranNewYybQz = "";
    public static void initCache(){
        Properties props = new Properties();
        Connection conn = null;
        try {
            props.load(GetNewYyb.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("pub_sys_url"), props);
            PreparedStatement ps = conn.prepareStatement("select param_val from t_sys_param where param_kind = '01' and ( param_code = 'IS_TRAN_NEW_YYB' or param_code = 'TRAN_NEW_YYB_QZ')");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                if (rs.isFirst()) {
                    isTranNewYyb = rs.getString("param_val");
                } else {
                    tranNewYybQz = rs.getString("param_val");
                }
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

    public Text evaluate(Text yyb, Text tranType) {
        if (yyb == null || tranType == null) {
            return null;
        }
        return change(yyb.toString(), tranType);
    }

    public Text evaluate(IntWritable yyb, Text tranType) {
        if (yyb == null || tranType == null) {
            return null;
        }
        return change(yyb.get() + "", tranType);
    }

    private Text change(String yyb, Text tranType) {
        String finalStr = "";
        try {
            if (tranType != null && !"".equals(tranType.toString())) {
                tranNewYybQz = tranType.toString();
            }
            if ("0".equals(isTranNewYyb)) {
                return new Text(yyb);
            }
            if (TO_NUMBER.equalsIgnoreCase(tranNewYybQz)) {
                return new Text(Integer.parseInt(yyb) + "");
            }
            String token = tranNewYybQz.split(":")[1];
            String[] split;
            if (tranNewYybQz.toUpperCase().startsWith(PREFIX)) {
                split = token.split("-");
                finalStr = StringUtil.fillStringLen(yyb, split[0], Integer.parseInt(split[1]));
                return new Text(finalStr);

            }
            if (tranNewYybQz.toUpperCase().startsWith(POSTFIX)) {
                split = token.split("-");
                finalStr = StringUtil.fillStringLenBack(yyb, split[0], Integer.parseInt(split[1]));
                return new Text(finalStr);
            }
            boolean matchFlag = false;
            if (tranNewYybQz.toUpperCase().startsWith(MAPP)) {
                split = token.split("\\|");
                if (split != null && split.length > 0) {
                    for (int i = 0; i < split.length; i++) {
                        if (yyb.equals(split[i].split("-")[0])) {
                            finalStr = split[i].split("-")[1];
                            matchFlag = true;
                            break;
                        }
                    }
                }
                //未匹配
                if (!matchFlag) {
                    finalStr = yyb;
                }
            }

        } catch (Exception e) {
            return new Text(yyb);
        }

        return new Text(finalStr);
    }
}

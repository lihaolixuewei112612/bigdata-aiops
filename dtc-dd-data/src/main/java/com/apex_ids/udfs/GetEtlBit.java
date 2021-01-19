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

/**
 * @author linqz
 */
@Description(
        name = "GetEtlBit",
        value = "_FUNC_(OWNER, TB_NAME, COL_NAME, IN_IS_BIT, OUT_IS_BIT ,DELIMITER_FLAG, N) - etl bit"
)
public class GetEtlBit extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetEtlBit.class);

    private static final String SPLIT_FLAG = "_";
    private static final String DEFAULT_DELIMITER = ",";
    // 设置上限 防止由于数据输入异常情况 陷入死循环
    private static final Integer BIT_LIMIT = 20;

    /**
     * 获取清洗错误标识位
     */
    private static String errFlag = "$";


    private static Map<String, Integer> dicQuoteMap = new HashMap<>();
    private static Map<String, Integer> dicInfoMap = new HashMap<>();
    public static void initCache(){
        Properties props = new Properties();
        Connection conn = null;
        Connection conn1 = null;
        try {
            props.load(GetEtlBit.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            //获取清洗错误标识位
            conn1 = DriverManager.getConnection(props.getProperty("pub_sys_url"), props);
            PreparedStatement ps = conn1.prepareStatement("select param_val from t_sys_param where param_kind = '01' and param_code = 'TRAN_ERR_FLAG'");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                errFlag = rs.getString("param_val");
            }
            //获取srcid
            conn = DriverManager.getConnection(props.getProperty("emc_pub_url"), props);
            ps = conn.prepareStatement("select owner,tb_name,col,src_id from t_stand_dic_quote");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicQuoteMap.put(rs.getString("owner") + SPLIT_FLAG + rs.getString("tb_name") + SPLIT_FLAG
                        + rs.getString("col"), rs.getInt("src_id"));
            }
            //获取dic info
            ps = conn.prepareStatement("select src_id,src_code,tar_code from t_stand_dic_info");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicInfoMap.put(rs.getInt("src_id") + SPLIT_FLAG + rs.getString("src_code"), rs.getInt("tar_code"));
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
            if (conn1 != null) {
                try {
                    conn1.close();
                } catch (SQLException e) {

                }
            }
        }
    }
    static {
        initCache();
    }

    public Text evaluate(Text owner, Text tbName, Text col, IntWritable inIsBit
            , IntWritable outIsBit, Text delimiter, IntWritable srcCode) {
        if (owner == null || tbName == null || col == null || inIsBit == null || outIsBit == null || srcCode == null) {
            return null;
        }
        return change(owner, tbName, col, inIsBit, outIsBit, delimiter, srcCode.get() + "");

    }

    public Text evaluate(Text owner, Text tbName, Text col, IntWritable inIsBit
            , IntWritable outIsBit, Text delimiter, Text srcCode) {
        if (owner == null || tbName == null || col == null || inIsBit == null || outIsBit == null || srcCode == null) {
            return null;
        }
        return change(owner, tbName, col, inIsBit, outIsBit, delimiter, srcCode.toString());

    }

    private Text change(Text owner, Text tbName, Text col, IntWritable inIsBit
            , IntWritable outIsBit, Text delimiter, String srcCode) {

        String delimiterFlag = DEFAULT_DELIMITER;
        if (delimiter != null && !"".equals(delimiter.toString())) {
            delimiterFlag = delimiter.toString();
        }
        Integer srcId = dicQuoteMap.get(owner.toString() + SPLIT_FLAG + tbName.toString() + SPLIT_FLAG + col.toString());
        if (srcId == null) {
            return null;
        }
        try {
            if (inIsBit.get() == 1) {
                //入参二进制
                long code = Long.parseLong(srcCode);
                long[] split = new long[2];
                long temp;
                //分割bit是否成功
                boolean splitFlag = false;
                for (int i = 0; i < BIT_LIMIT; i++) {
                    temp = 1 << i;
                    if ((temp & code) > 0 && (code - temp == 0 || nCF(code - temp))) {
                        split[0] = temp;
                        split[1] = code - temp;
                        splitFlag = true;
                        break;
                    }
                }
                if (!splitFlag) {
                    return null;
                }
                Integer s1 = dicInfoMap.get(srcId + SPLIT_FLAG + split[0]);
                Integer s2 = dicInfoMap.get(srcId + SPLIT_FLAG + split[1]);
                if (s1 == null && s2 == null) {
                    return new Text(errFlag + srcCode);
                }
                if (outIsBit.get() == 1) {
                    return new Text((s1 == null ? 0 : s1) + (s2 == null ? 0 : s2) + "");
                } else {
                    return new Text(s1 == null ? (s2 == null ? "" : s2 + "") : (s2 == null ? s1 + "" : s1 + delimiterFlag + s2));
                }
            } else {
                String[] split = srcCode.split(delimiterFlag);
                if (split != null && split.length > 0) {
                    Integer temp;
                    if (outIsBit.get() == 1) {
                        int result = 0;
                        for (int i = 0; i < split.length; i++) {
                            temp = dicInfoMap.get(srcId + SPLIT_FLAG + split[i]);
                            if (temp != null) {
                                result += temp;
                            }
                        }
                        if (result > 0) {
                            return new Text(result + "");
                        }
                    } else {
                        StringBuilder result = new StringBuilder("");
                        for (int i = 0; i < split.length; i++) {
                            temp = dicInfoMap.get(srcId + SPLIT_FLAG + split[i]);
                            if (temp != null) {
                                result.append(temp + delimiterFlag);
                            }
                        }
                        if (result.length() > 0) {
                            return new Text(result.substring(0, result.length() - delimiterFlag.length()));
                        }
                    }
                }
            }
        } catch (Exception e) {
            return null;
        }
        return new Text(errFlag + srcCode);
    }

    private boolean nCF(long n) {
        boolean b;
        while (true) {
            long j = n % 2;
            n = n / 2;
            if (j == 1) {
                b = false;
                break;
            }
            if (n == 2) {
                b = true;
                break;
            }
        }
        return b;
    }

}

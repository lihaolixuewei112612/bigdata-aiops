package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author linqz
 */
@Description(
        name = "GetRateParam",
        value = "_FUNC_(bz,date) - 获取汇率参数 "
)
public class GetRateParam extends UDF {
    private static final Logger log = LoggerFactory.getLogger(GetRateParam.class);

    private static final String SPLIT_FLAG = "_";

    private static Map<String, BigDecimal> dicParamMap = new HashMap<>(10);
    public static void initCache(){
        dicParamMap.clear();
        Properties props = new Properties();
        Connection conn = null;
        try {
            props.load(GetEtlBit.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            //获取汇率参数
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg_url"), props);
            PreparedStatement ps = conn.prepareStatement("select bz,gsbl,ksrq,jsrq from t_hlcs ");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                dicParamMap.put(rs.getString("bz") + SPLIT_FLAG + rs.getInt("ksrq") + SPLIT_FLAG
                        + rs.getInt("jsrq"), rs.getBigDecimal("gsbl"));
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

    public Double evaluate(String bz, Integer date) {
        if (bz == null || date == null) {
            return null;
        }
        String[] arr;
        for (Map.Entry<String, BigDecimal> entry : dicParamMap.entrySet()) {
            arr = entry.getKey().split(SPLIT_FLAG);
            if (bz.toString().equalsIgnoreCase(arr[0]) && Integer.parseInt(arr[1]) <= date
                    && Integer.parseInt(arr[2]) >= date) {
                return entry.getValue().doubleValue();
            }
        }
        return new Double(1.0);
    }


}

package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.Map;

/**
 * Created by 守门员 on 2018/7/24.
 */
@Description(
        name = "GetEtlBsdirection",
        value = "__FUNC__(srcMarketCode,srcTradeType,tranType)"
)
public class GetEtlBsdirection extends UDF{
    private static final Logger log = LoggerFactory.getLogger(GetEtlBsdirection.class);
    private static final String SPLIT_FLAG = "_";
    private static Map<String, String> dicConfig = new HashMap<>();
    public static void initCache(){
        Properties prop = new Properties();
        Connection conn = null;
        try {
            prop.load(GetEtlBsdirection.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(prop.getProperty("driver"));
            conn = DriverManager.getConnection(prop.getProperty("emc_pub_url"), prop);
            PreparedStatement ps = conn.prepareStatement("select market,bsflag,funddirect,stkdirect from bsconfig");
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                dicConfig.put(rs.getString("market") + SPLIT_FLAG + rs.getString("bsflag"), rs.getString("funddirect") + SPLIT_FLAG + rs.getString("stkdirect"));
            }
        }catch (Exception e){
            log.error(e.getMessage());
        }finally {
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
    public IntWritable evaluate(String srcMarketCode, String srcTradeType, String tranType){
        String tar_code = dicConfig.get(srcMarketCode + SPLIT_FLAG + srcTradeType);
        if (tar_code == null){
            return null;
        }
        if (tranType.equals("ZJ")){
            if (tar_code.split(SPLIT_FLAG)[0].equals("M")){
                return new IntWritable(-1);
            }else {
                return new IntWritable(1);
            }
        }else{
            if (tar_code.split(SPLIT_FLAG)[1].equals("M")){
                return new IntWritable((-1));
            }else{
                return new IntWritable(1);
            }

        }
    }
}

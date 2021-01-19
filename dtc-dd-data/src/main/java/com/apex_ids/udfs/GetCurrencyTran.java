package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.String;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
/**
 * @author ChenTong
 */
@Description(
        name = "initProfitRate",
        value = "_FUNC_(Double, String) - 根据类型返回Double"
)

public class GetCurrencyTran extends UDF{
    private static final Logger log = LoggerFactory.getLogger(GetCurrencyTran.class);
    public static Map<String, Double> currencyMap;

    static {
        initCache();
    }
    public static void initCache(){
        Properties props = new Properties();
        Connection conn = null;
        try{
            props.load(GetEtlDic.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("dsc_cfg"), props);
            PreparedStatement ps = conn.prepareStatement("select bz, gsbl from t_hcls");
            ResultSet rs = ps.executeQuery();
            while(rs.next()){
                currencyMap.put(rs.getString("bz"), rs.getDouble("gsbl"));
            }
        }
        catch (Exception e){
            log.error(e.getMessage());
        }
        finally{
            if(conn != null){
                try{
                    conn.close();
                }
                catch(SQLException e){

                }
            }
        }
    }

    public DoubleWritable evaluate(DoubleWritable value, Text currency) throws Exception{
        try {
            return new DoubleWritable(currency.equals("1") ? Double.valueOf(value.toString()) : Double.valueOf(value.toString()) * (Double)currencyMap.get(currency.toString()));
        } catch (Exception var4) {
            return value;
        }
    }
}

package com.apex_ids.udfs;

import org.apache.commons.lang.StringUtils;
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
 * Created by 守门员 on 2018/7/23.
 */
@Description(
        name = "GetEtlZqlbDic",
        value = "_FUNC_(owner, tbName, col, isErrOldValue, isTranFlag, srcExchangeCode, srcCategoryCode, srcTradeCode)"
)
public class GetEtlZqlbDic extends UDF{
    private static final Logger log = LoggerFactory.getLogger(GetEtlZqlbDic.class);
    private static final String SPLIT_FLAG = "_";
    private static String errFlag = "$";
    private static Map<String, Integer> dicQuoteMap = new HashMap<>();
    private static Map<String, String> dicInfoMap = new HashMap<>();
    public static void initCache(){
        Properties props = new Properties();
        Connection conn = null;
        Connection conn1 = null;
        try {
            props.load(GetEtlZqlbDic.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("pub_sys_url"), props);
            //获取清洗错误标识位
            PreparedStatement ps = conn.prepareStatement("select param_val from t_sys_param where param_kind = '01' and param_code = 'TRAN_ERR_FLAG'");
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                errFlag = rs.getString("param_val");
            }
            //获取srcid
            conn1 = DriverManager.getConnection(props.getProperty("emc_pub_url"), props);
            ps = conn1.prepareStatement("select owner,tb_name,col,src_id from t_stand_dic_quote");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicQuoteMap.put(rs.getString("owner") + SPLIT_FLAG + rs.getString("tb_name") + SPLIT_FLAG
                        + rs.getString("col"), rs.getInt("src_id"));
            }
            //获取dic info
            ps = conn1.prepareStatement("select src_id,src_market,src_stktype,src_trdid,tar_code from t_stand_dic_zqlb_info");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicInfoMap.put(rs.getInt("src_id")+ SPLIT_FLAG + rs.getString("src_market") + SPLIT_FLAG + rs.getString("src_stktype") + SPLIT_FLAG + rs.getString("src_trdid"), rs.getString("tar_code"));
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

    public Text evaluate(Text owner, Text tbName, Text col, IntWritable isErrOldValue,
                         IntWritable isTranFlag, Text srcExchangeCode, Text srcCategoryCode, Text srcTradeCode){
        return change(owner, tbName, col, isErrOldValue, isTranFlag, srcExchangeCode, srcCategoryCode, srcTradeCode.toString());
    }
    public Text evaluate(Text owner, Text tbName, Text col, IntWritable isErrOldValue,
                         IntWritable isTranFlag, Text srcExchangeCode, Text srcCategoryCode, IntWritable srcTradeCode){
        return change(owner, tbName, col, isErrOldValue, isTranFlag, srcExchangeCode, srcCategoryCode, srcTradeCode.toString());
    }
    private Text change(Text owner, Text tbName, Text col, IntWritable isErrOldValue,
                        IntWritable isTranFlag, Text srcExchangeCode, Text srcCategoryCode, String srcTradeCode){
        Integer src_id = dicQuoteMap.get(owner.toString() + SPLIT_FLAG + tbName.toString() + SPLIT_FLAG + col.toString());
        if (src_id == null){
            return null;
        }
        String tar_code = dicInfoMap.get(src_id + SPLIT_FLAG + srcExchangeCode + SPLIT_FLAG + srcCategoryCode + SPLIT_FLAG + srcTradeCode);
        if (StringUtils.isBlank(tar_code)){
            if (isErrOldValue.get() == 1 && isTranFlag.get() == 1) {
                return new Text(errFlag + srcCategoryCode);
            }
            if (isErrOldValue.get() == 1) {
                return new Text(srcCategoryCode);
            }
            return new Text("");
        }
        return new Text(tar_code);
    }

}

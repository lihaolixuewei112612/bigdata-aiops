package com.apex_ids.udfs.edw;

import org.apache.commons.lang3.StringUtils;
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
        name = "GetEtlDic",
        value = "_FUNC_(OWNER, TB_NAME, COL_NAME, IS_ERR_OLD_VALUE, IS_TRAN_FLAG ,N) - dic clear "
)
public class GetEtlDic extends UDF {
    private static final Logger logger = LoggerFactory.getLogger(GetEtlDic.class);

    private static final String SPLIT_FLAG = "_";
    private static String errFlag = "$";

    private static Map<String, Integer> dicQuoteMap = new HashMap<>();
    private static Map<String, String> dicInfoMap = new HashMap<>();
    public static void initCache(){
        dicQuoteMap.clear();
        dicInfoMap.clear();
        Properties props = new Properties();
        Connection conn = null;
        Connection conn1 = null;
        PreparedStatement ps=null;
        ResultSet rs=null;
        try {
            props.load(GetEtlDic.class.getResourceAsStream("/jdbc.properties"));
            Class.forName(props.getProperty("driver"));
            conn = DriverManager.getConnection(props.getProperty("pub_sys_url"), props);
            //获取清洗错误标识位
            ps = conn.prepareStatement("select param_val from t_sys_param where param_kind = '01' and param_code = 'TRAN_ERR_FLAG'");
            rs = ps.executeQuery();
            while (rs.next()) {
                errFlag = rs.getString("param_val");
            }
            //获取srcid
            conn1 = DriverManager.getConnection(props.getProperty("emc_pub_url"), props);
            ps = conn1.prepareStatement("select owner,tb_name,col,src_id from t_stand_dic_quote_ht_edw");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicQuoteMap.put(rs.getString("owner") + SPLIT_FLAG + rs.getString("tb_name") + SPLIT_FLAG
                        + rs.getString("col"), rs.getInt("src_id"));
            }
            //获取dic info
            ps = conn1.prepareStatement("select src_id,src_code,tar_code from t_stand_dic_info_ht_edw");
            rs = ps.executeQuery();
            while (rs.next()) {
                dicInfoMap.put(rs.getInt("src_id") + SPLIT_FLAG + rs.getString("src_code"), rs.getString("tar_code"));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(rs!=null){
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps!=null){
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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

    public Text evaluate(Text owner, Text tbName, Text col, IntWritable isErrOldValue
            , IntWritable isTranFlag, Text srcCode) {
        if (owner == null || tbName == null || col == null || isErrOldValue == null || isTranFlag == null || srcCode == null) {
            return null;
        }
        return change(owner, tbName, col, isErrOldValue, isTranFlag, srcCode.toString());

    }

    public Text evaluate(Text owner, Text tbName, Text col, IntWritable isErrOldValue
            , IntWritable isTranFlag, IntWritable srcCode) {
        if (owner == null || tbName == null || col == null || isErrOldValue == null || isTranFlag == null || srcCode == null) {
            return null;
        }
        return change(owner, tbName, col, isErrOldValue, isTranFlag, srcCode.get() + "");
    }

    private Text change(Text owner, Text tbName, Text col, IntWritable isErrOldValue
            , IntWritable isTranFlag, String srcCode) {
        Integer srcId = dicQuoteMap.get(owner.toString() + SPLIT_FLAG + tbName.toString() + SPLIT_FLAG + col.toString());
        if (srcId == null) {
            return null;
        }
        String tarCode =null;
        if(!StringUtils.isBlank(dicInfoMap.get(srcId + SPLIT_FLAG + srcCode))) {
            tarCode = String.valueOf(dicInfoMap.get(srcId + SPLIT_FLAG + srcCode));
        }
        //if (tarCode == null || "".equals(tarCode)) {
        if (tarCode == null) {
            if (isErrOldValue.get() == 1 && isTranFlag.get() == 1) {
                return new Text(errFlag + srcCode);
            }
            if (isErrOldValue.get() == 1) {
                return new Text(srcCode);
            }
            return new Text("");
        }
        return new Text(tarCode);
    }


}

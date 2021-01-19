package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhangwz
 */
@Description(
        name = "InvalidateCache",
        value = "_FUNC_(func) - 刷新缓存 "
)
public class InvalidateCache extends UDF {
    private static final Logger log = LoggerFactory.getLogger(InvalidateCache.class);

    public int evaluate(String func){
        try {
            switch (func.toLowerCase()){
                case "f_get_jyr_date":
                    TradingDays.initCache();
                    break;
                case "f_get_new_yyb":
                    GetNewYyb.initCache();
                    break;
                case "f_get_etl_tran_bitdicval":
                    GetEtlBit.initCache();
                    break;
                case "f_get_etl_bsdirection":
                    GetEtlBsdirection.initCache();
                    break;
                case "f_get_etl_tran_dicval":
                    GetEtlDic.initCache();
                    break;
                case "f_get_hlcs":
                    GetRateParam.initCache();
                    break;
                case "f_get_etl_tran_zqlb_dicval":
                    GetEtlZqlbDic.initCache();
                    break;
                case "f_get_paramvalue":
                    GetSysConfig.initCache();
                    break;
            }
            return 1;
        }catch (Exception e){
            log.error(e.getMessage());
            return 0;
        }
    }
}

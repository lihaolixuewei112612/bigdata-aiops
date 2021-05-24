/*
	@@name:个股期权合约行情表转换
	@@componet_type: inceptor
	@@author:李伟
	@@version: V1.0.0
    @@update_time:2018-12-17
	@@comment:改为inceptor格式
			EMC_APEX 修改：插入分区表时去除插入列名列表中的分区字段
	修改：20190729 HYDW字段优先SRC_ABOSS.TSO_QQHQ表获取，获取不要从SRC_ABOSS.TSO_HYDM表获取
	@@注意事项：1、SRC_ABOSS.TSO_HYDM表到期的会保留五天然后删除
	            2、SRC_ABOSS.TSO_QQHQ表中的HYDW是后面新增字段，历史数据没有处理赋值为0
 */

INSERT OVERWRITE TABLE DSC_BAS.T_SO_HYHQ_HIS
    PARTITION
    (RQ = ${hivevar:RQ})(JYS, HYDM, JYDW, ZXJ, JSJ, ZSP, ZJSJ, JKP, ZGJ, ZDJ, CJSL, CJJE)
    SELECT F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_SO_HYHQ_HIS', 'JYS', 1, 1, A.JYS) AS JYS,
           A.HYDM,
           CASE
               WHEN A.HYDW IS NULL OR A.HYDW = 0 THEN
                NVL(T.HYDW, 10000)
               ELSE
                A.HYDW
           END AS HYDW,
           CAST(A.ZXJ AS DECIMAL(9, 4)) AS ZXJ,
           A.JSJ,
           CAST(A.ZSP AS DECIMAL(9, 4)) AS ZSP,
           CAST(A.ZJSJ AS DECIMAL(9, 4)) AS ZJSJ,
           CAST(A.JKP AS DECIMAL(9, 4)) AS JKP,
           CAST(A.ZGJ AS DECIMAL(9, 4)) AS ZGJ,
           CAST(A.ZDJ AS DECIMAL(9, 4)) AS ZDJ,
           A.CJSL,
           A.CJJE
      FROM SRC_ABOSS.TSO_QQHQ A
      LEFT JOIN SRC_ABOSS.TSO_HYDM T
        ON A.JYS = T.JYS
       AND A.HYDM = T.HYDM
     WHERE A.RQ = ${hivevar:RQ};

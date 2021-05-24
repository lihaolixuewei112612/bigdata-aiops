/*
	@@name:权益代码表清洗转换过程
	@@componet_type: impala
	@@author:李伟
	@@version: V1.0.0
    @@update_time:2019-08-21
	@@comment:EMC_APEX
	注意事项：该脚本依赖金融数据
	修改记录
	---------------------------------------
	李伟     20190906      修改：1、权益代码只存储配股和可转债配售的权益代码信息
	                             2、配股和可转债的SSRQ字段可能为null或者0，需要对该类数据赋默认值

*/
set character.literal.as.string=true;
-- 获取权益信息
INSERT OVERWRITE TABLE DSC_BAS.T_QYDM
    (QYDM, ZQDM, JYS, QYLX)
    SELECT RGDM AS QYDM,
           ZQDM,
           CASE
               WHEN SUBSTR(GPDM, 1, 1) = '6' THEN
                '2'
               WHEN SUBSTR(GPDM, 1, 1) IN ('0', '3') THEN
                '1'
               ELSE
                CONCAT('$', SUBSTR(GPDM, 1, 1))
           END AS JYS,
           CAST('KZZ_RG' AS STRING) AS QYLX
      FROM INFO.TZQ_KZZ K
     WHERE ${hivevar:RQ} BETWEEN F_GET_JYR_DATE(CAST(K.SGRQ AS INT), -1) AND
           F_GET_JYR_DATE(CAST((CASE
                                        WHEN NVL(K.SSRQ, 0) = 0 THEN
                                         30000101
                                        ELSE
                                         K.SSRQ
                                    END) AS INT),
                               -1)
       AND K.RGDM IS NOT NULL
       AND K.RGDM <> ''
       AND NVL(K.SGRQ, 0) <> 0
    UNION ALL
    SELECT PSDM AS QYDM, ZQDM, JYS, 'GPPG' AS QYLX
      FROM INFO.TGP_GPPG P
     WHERE ${hivevar:RQ} BETWEEN CAST(P.GQDJRQ AS INT) AND
           F_GET_JYR_DATE(CAST((CASE
                                        WHEN NVL(P.PGSSRQ, 0) = 0 THEN
                                         30000101
                                        ELSE
                                         P.PGSSRQ
                                    END) AS INT),
                               -2)
       AND P.PSDM IS NOT NULL
       AND P.PSDM <> ''
       AND NVL(P.GQDJRQ, 0) <> 0;

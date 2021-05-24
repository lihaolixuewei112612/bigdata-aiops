/*
	@@name: 证券代码表清洗转换过程
	@@componet_type: impala
	@@author:李伟
	@@version: V1.0.0
    @@update_time:2017-12-25
	@@comment:EMC_APEX 修改：20190724 添加jjjybz字段的赋值
	修改记录
	-----------------------------
	李伟    20190911     修改：新增ZXLX字段，并给该字段赋值
 */

set character.literal.as.string=true;
--STEP0：将源表[SRC_ABOSS.TZQDM]的数据清洗转换到目标表[DSC_CFG.T_ZQDM]中
INSERT OVERWRITE TABLE DSC_CFG.T_ZQDM
    (JYS,
     ZQDM,
     ZQMC,
     PYDM,
     ZQLB,
     JYDW,
     ZQQC,
     FXRQ,
     DFRQ,
     JYZT,
     BZ,
     GT_ZQLB,
     JJJYBZ,
     ZXLX)
    SELECT F_GET_ETL_TRAN_DICVAL('DSC_CFG', 'T_ZQDM', 'JYS', 1, 1, A.JYS) AS JYS,
           TRIM(ZQDM),
           ZQMC,
           PYDM,
           ZQLB,
           JYDW,
           ZQQC,
           FXRQ,
           DFRQ,
           CAST(JYZT AS STRING) AS JYZT,
           F_GET_ETL_TRAN_DICVAL('DSC_CFG', 'T_ZQDM', 'BZ', 1, 1, A.BZ) AS BZ,
           ZQLB AS GT_ZQLB,
           JJJYBZ,
           ZXLX
      FROM SRC_ABOSS.TZQDM A;

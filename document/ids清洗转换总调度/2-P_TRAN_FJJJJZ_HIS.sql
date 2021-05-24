/*
	@@name:分级基金行情清洗转换程序
	@@componet_type: inceptor
	@@author:李伟
	@@version: V1.0.1
    @@update_time:2019-07-24
	@@comment:EMC_APEX
*/

-- 获取前一个交易日
SET hivevar:JYR_PRE1=F_GET_JYR_DATE(${hivevar:RQ}, -1);

--STEP 0:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_FJJJJZ_HIS;

--STEP 1:创建临时表
CREATE TABLE TEMP.TEMP_T_FJJJJZ_HIS
AS
SELECT * FROM DSC_BAS.T_FJJJJZ_HIS
WHERE 1 = 0;

--1.1 基金净值处理
INSERT INTO TEMP.TEMP_T_FJJJJZ_HIS
    (RQ, TADM, JJDM, JJJZ, LJJZ, MWFSY, NSYL, YHSYL, SYLPCJD, GSJZ, FHPS, ISGSJZ, JZRQ)
    SELECT ${hivevar:RQ} AS RQ,
           TADM,
           JJDM,
           JJJZ,
           LJJZ,
           MWFSY,  --每日万分收益
           NSYL,   --年化收益率
           CAST(YHSYL AS DECIMAL(10, 6)) AS YHSYL,   --隐含收益率
           CAST(SYLPCJD AS DECIMAL(10, 6)) AS SYLPCJD,  --收益率偏差基点
           CAST(GSJZ AS DECIMAL(9, 6)) AS GSJZ,       --B基/母基估算净值
           FHPS,      --分红配送
           0 AS ISGSJZ,
           ${hivevar:RQ} AS JZRQ
      FROM INFO.THIS_JJJZ --分级基金历史净值
     WHERE JZRQ = ${hivevar:RQ};

--1.2 分级基金B基/母基的基金净值处理
INSERT INTO TEMP.TEMP_T_FJJJJZ_HIS
    (RQ, TADM, JJDM, JJJZ, LJJZ, MWFSY, NSYL, YHSYL, SYLPCJD, GSJZ, FHPS, ISGSJZ, JZRQ)
    SELECT ${hivevar:RQ} AS RQ,
           X.JYS AS TADM,    --交易所
           X.MJDM AS JJDM,   --母基金代码
           CAST(ROUND(J.JJJZ * (1 + X.ZSZD * X.GSCW / 100), 4) AS DECIMAL(9, 4)) AS JJJZ,
           CAST(ROUND(J.JJJZ * (1 + X.ZSZD * X.GSCW / 100), 4) AS DECIMAL(12, 4)) AS LJJZ,
           CAST(0 AS DECIMAL(10, 4)) AS MWFSY,
           CAST(0 AS DECIMAL(10, 4)) AS NSYL,
           CAST(0 AS DECIMAL(10, 6)) AS YHSYL,
           CAST(0 AS DECIMAL(10, 6)) AS SYLPCJD,
           CAST(0 AS DECIMAL(9, 6)) GSJZ,
           CAST(NULL AS DECIMAL(9, 6)) AS FHPS,
           1 AS ISGSJZ,
           ${hivevar:RQ} AS JZRQ
      FROM INFO.TFJJJXX X,  --分级基金信息
           (SELECT JJJZ, TADM, JJDM FROM INFO.THIS_JJJZ WHERE JZRQ = ${hivevar:JYR_PRE1}) J  --上一日的分级基金净值
     WHERE X.JYBS = 0  --不可交易
       AND X.JYS = J.TADM
       AND X.MJDM = J.JJDM
       AND NOT EXISTS (SELECT 1
              FROM INFO.THIS_JJJZ T
             WHERE T.TADM = X.JYS
               AND T.JJDM = X.MJDM
               AND T.JZRQ = ${hivevar:RQ});

--STEP 2 将临时表的数据插入目标表
INSERT OVERWRITE TABLE DSC_BAS.T_FJJJJZ_HIS
    PARTITION
    (JZRQ = ${hivevar:RQ})
    (RQ, TADM, JJDM, JJJZ, LJJZ, MWFSY, NSYL, YHSYL, SYLPCJD, GSJZ, FHPS, ISGSJZ)
    SELECT RQ, TADM, JJDM, JJJZ, LJJZ, MWFSY, NSYL, YHSYL, SYLPCJD, GSJZ, FHPS, ISGSJZ
      FROM TEMP.TEMP_T_FJJJJZ_HIS;

--暂时屏蔽	20190417
--INSERT INTO INFO.THGCPXX (JYS,ZQDM,BZMC,TS)
--SELECT JYS,ZQDM,BZMC,TS FROM INFO.TBJHGCPXX B
--WHERE NOT EXISTS(SELECT 1 FROM INFO.THGCPXX H WHERE B.JYS=H.JYS AND B.ZQDM=H.ZQDM);

--STEP 99:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_FJJJJZ_HIS;

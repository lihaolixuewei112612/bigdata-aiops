/*
 @@name:证券余额转换
 @@componet_type: impala
 @@author:李伟
 @@version: V1.0.0
    @@update_time:2018-12-17
 @@comment:EMC_APEX 修改：插入分区表时去除插入列名列表中的分区字段
 注意事项：
 1、该脚本依赖dsc_bas.t_zqhq_xz_his、dsc_bas.t_qydm_zqye_his和dsc_bas.t_qydm表
 修改记录
 ------------------------------------------
 李伟      20190725    修改：对新增gt_zxsz字段进行赋值;调整最新市值算法zxsz=证券数量*行情
 李伟      20190805    修改：证券品种扩展为4位
 李伟      20190821    修改：调整权益代码持仓的取数,从dsc_bas.t_qydm_zqye_his表
 李伟      20190906    修改：部分证券代码无法从行情表获取交易单位，从dsc_cfg.t_zqlb表获取
 李伟      20190923    修改：无法从行情表获取行情，最新市值保留原始值
 李伟      20200305    修改：行情修正表中ZXLX和LXJG字段存储是最新利息，精度不同，ZXLX字段的精度更准确些,最新利息调整为ZXLX字段获取
                             行情表中对港股的最新价没有修正，港股的市值需要乘以汇率
*/
set character.literal.as.string=true;
-----STEP 0:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_01;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_02;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_03;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_04;

-----STEP 1:创建临时表，并将SRC层的交割流水数据进行清洗转换生成进去
CREATE TABLE TEMP.TEMP_T_ZQYE_01
AS
SELECT * FROM DSC_BAS.T_ZQYE WHERE 1=0;

--1.1 修正证券数量和最新市值
--证券数量乘以交易单位，主要对上海债券(上海债券存储单位手 1手=10股)有影响
--最新市值=证券数量*交易单位*行情
INSERT INTO TEMP.TEMP_T_ZQYE_01
    (RQ,
     KHH,
     KHXM,
     YYB,
     JYS,
     GDH,
     ZQDM,
     XWDM,
     ZQMC,
     ZQLB,
     BZ,
     ZQSL,
     DJSL,
     FLTSL,
     WJSSL,
     ZXSZ,
     KCRQ,
     BDRQ,
     MRJE,
     MRSL,
     PGJE,
     PGSL,
     MCJE,
     MCSL,
     HLJE,
     SGSL,
     JYFY,
     CCCB,
     LJYK,
     GT_ZXSZ)
    SELECT T1.RQ,
           T1.KHH,
           T1.KHXM,
           T1.YYB,
           T1.JYS,
           T1.GDH,
           T1.ZQDM,
           T1.XWDM,
           T1.ZQMC,
           T1.ZQLB,
           T1.BZ,
           CASE
               WHEN T1.ZXSZ <> 0 AND T1.ZQSL = 0 THEN --20191017修正证券数量 = 最新市值/最新价
                 CAST(ROUND(T1.ZXSZ / (CASE WHEN NVL(T2.JJJYBZ,0) = 1 THEN 
                                    (NVL(T2.ZXJ,0) + NVL(T2.ZXLX,0))
                                    ELSE
                                     NVL(T2.ZXJ,0)
                                     END),
                                     2) AS DECIMAL(22,2))
                    ELSE
                     T1.ZQSL * COALESCE(T2.JYDW, T3.JYDW,1)
            END AS ZQSL,
           T1.DJSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS DJSL,
           T1.FLTSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS FLTSL,
           T1.WJSSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS WJSSL,
           CASE
               WHEN T1.ZXSZ <> 0 AND T1.ZQSL = 0 THEN --20191017 证券数量=0 最新市值 ！= 0 保留原值
               T1.ZXSZ
                WHEN T2.ZQDM IS NOT NULL AND T1.JYS IN ('8', '9') THEN -- 港股有行情数据
                CAST(ROUND(T1.ZQSL * COALESCE(T2.JYDW, T3.JYDW, 1) * (CASE
                               WHEN NVL(T2.JJJYBZ, 0) = 1 THEN
                                (NVL(T2.ZXJ, 0) + NVL(T2.ZXLX, 0)) -- 20200305
                               ELSE
                                NVL(T2.ZXJ, 0)
                           END) * NVL(HL.XHMRJ, 1), --港股市值需要乘汇率
                           2) AS DECIMAL(22, 2))
               WHEN T2.ZQDM IS NOT NULL AND T2.ZXJ is NOT NULL THEN -- 有行情数据
                CAST(ROUND(T1.ZQSL * COALESCE(T2.JYDW, T3.JYDW, 1) * (CASE 
                               WHEN NVL(T2.JJJYBZ, 0) = 1 THEN
                                (NVL(T2.ZXJ, 0) + NVL(T2.ZXLX, 0))
                               ELSE
                                NVL(T2.ZXJ, 0)
                           END),
                           2) AS DECIMAL(22, 2))
               WHEN T1.JYS IN ('8', '9') THEN --港股市值需要乘汇率-- 无行情数据
                CAST(ROUND(T1.ZXSZ * NVL(HL.XHMRJ, 1), 2) AS DECIMAL(22, 2))
               ELSE
                T1.ZXSZ
           END AS ZXSZ,
           T1.KCRQ,
           T1.BDRQ,
           T1.MRJE,
           T1.MRSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS MRSL,
           T1.PGJE,
           T1.PGSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS PGSL,
           T1.MCJE,
           T1.MCSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS MCSL,
           T1.HLJE,
           T1.SGSL * COALESCE(T2.JYDW, T3.JYDW, 1) AS SGSL,
           T1.JYFY,
           T1.CCCB,
           T1.LJYK,
           T1.ZXSZ AS GT_ZXSZ
      FROM (SELECT ${hivevar:RQ} AS RQ,
                   KHH,
                   KHXM,
                   F_GET_NEW_YYB(A.YYB, '') AS YYB,
                   F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_ZQYE', 'JYS', 1, 1, A.JYS) AS JYS,
                   GDH,
                   ZQDM,
                   XWDM,
                   ZQMC,
                   ZQLB,
                   F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_ZQYE', 'BZ', 1, 1, A.BZ) AS BZ,
                   ZQSL,
                   DJSL,
                   FLTSL,
                   WJSSL,
                   ZXSZ,
                   KCRQ,
                   BDRQ,
                   MRJE,
                   MRSL,
                   PGJE,
                   PGSL,
                   MCJE,
                   MCSL,
                   HLJE,
                   SGSL,
                   JYFY,
                   CCCB,
                   LJYK
              FROM SRC_ABOSS.TZQGL A) T1
      LEFT JOIN DSC_BAS.T_ZQHQ_XZ_HIS T2 --证券行情修正表
        ON T1.JYS = T2.JYS
       AND T1.ZQDM = T2.ZQDM
       AND T2.RQ = ${hivevar:RQ}
      LEFT JOIN DSC_CFG.T_ZQLB T3
        ON T1.JYS = T3.JYS
       AND T1.ZQLB = T3.ZQLB
      LEFT JOIN DSC_CFG.T_HLCS_EXT HL
        ON HL.BZ = '2'
        and 1 = 1
       AND HL.RQ = ${hivevar:RQ};


-----STEP 2：证券品种处理
-----STEP 2.1：排除中小板
CREATE TABLE TEMP.TEMP_T_ZQYE_02
AS
SELECT T.RQ,
       T.KHH,
       T.KHXM,
       T.YYB,
       T.JYS,
       T.GDH,
       T.ZQDM,
       T.XWDM,
       T.ZQMC,
       T.ZQLB,
       T.BZ,
       T.ZQSL,
       T.DJSL,
       T.FLTSL,
       T.WJSSL,
       T.ZXSZ,
       T.KCRQ,
       T.BDRQ,
       T.MRJE,
       T.MRSL,
       T.PGJE,
       T.PGSL,
       T.MCJE,
       T.MCSL,
       T.HLJE,
       T.SGSL,
       T.JYFY,
       T.CCCB,
       T.LJYK,
       T.ZY,
    T.GT_ZXSZ,
       CASE
           WHEN NOT (T.ZQDM LIKE '002%' AND T.ZQLB = 'A0' AND T.JYS = '1') THEN
            T1.ZQPZ
       END AS ZQPZ
  FROM TEMP.TEMP_T_ZQYE_01 T
  LEFT JOIN (SELECT DISTINCT JYS, ZQLB, JB3_ZQPZ AS ZQPZ
               FROM DSC_CFG.T_ZQPZ_DY D
              WHERE D.JB3_ZQPZ <> '1102'
                AND IS_VALID = 1) T1
    ON (T.JYS = T1.JYS AND T.ZQLB = T1.ZQLB);


-----STEP 2.2：处理中小板和基金部分
CREATE TABLE TEMP.TEMP_T_ZQYE_03
AS
SELECT T.RQ,
       T.KHH,
       T.KHXM,
       T.YYB,
       T.JYS,
       T.GDH,
       T.ZQDM,
       T.XWDM,
       T.ZQMC,
       T.ZQLB,
       T.BZ,
       T.ZQSL,
       T.DJSL,
       T.FLTSL,
       T.WJSSL,
       T.ZXSZ,
       T.KCRQ,
       T.BDRQ,
       T.MRJE,
       T.MRSL,
       T.PGJE,
       T.PGSL,
       T.MCJE,
       T.MCSL,
       T.HLJE,
       T.SGSL,
       T.JYFY,
       T.CCCB,
       T.LJYK,
       T.ZY,
    T.GT_ZXSZ,
       CASE
           WHEN T.ZQDM LIKE '002%' AND T.ZQLB = 'A0' AND T.JYS = '1' THEN
            T1.ZQPZ
           WHEN T.ZQPZ = '2422' THEN
            NVL(T2.ZQPZ, '2422')
           ELSE
            T.ZQPZ
       END AS ZQPZ
  FROM TEMP.TEMP_T_ZQYE_02 T
  LEFT JOIN (SELECT DISTINCT JYS, ZQLB, JB3_ZQPZ AS ZQPZ
               FROM DSC_CFG.T_ZQPZ_DY D
              WHERE D.JB3_ZQPZ = '1102'   --中小板
                AND IS_VALID = 1) T1
    ON (T.JYS = T1.JYS AND T.ZQLB = T1.ZQLB)
  LEFT JOIN (SELECT PARAM_VALUE AS ZQPZ, PARAM_VALUE2 AS ZQDM
               FROM EMC_PUB.T_PARAM_VALUE
              WHERE PARAM_KIND = '01') T2
    ON T.ZQDM = T2.ZQDM;


--STEP 3：过滤上市后配股和债券市值以及权益代码的持仓
CREATE TABLE TEMP.TEMP_T_ZQYE_04
AS
SELECT T.RQ,
       T.KHH,
       T.KHXM,
       T.YYB,
       T.JYS,
       T.GDH,
       T.ZQDM,
       T.XWDM,
       T.ZQMC,
       T.ZQLB,
       T.BZ,
       T.ZQSL,
       T.DJSL,
       T.FLTSL,
       T.WJSSL,
       T.ZXSZ,
       T.KCRQ,
       T.BDRQ,
       T.MRJE,
       T.MRSL,
       T.PGJE,
       T.PGSL,
       T.MCJE,
       T.MCSL,
       T.HLJE,
       T.SGSL,
       T.JYFY,
       T.CCCB,
       T.LJYK,
       T.ZY,
       T.GT_ZXSZ,
       T.ZQPZ
  FROM TEMP.TEMP_T_ZQYE_03 T
 WHERE NOT EXISTS (SELECT 1
          FROM INFO.TGP_GPPG A -- 过滤配股上市的持仓
         WHERE F_GET_JYR_DATE(A.PGSSRQ, -1) <= ${hivevar:RQ}
           AND T.JYS = A.JYS
           AND T.ZQDM = A.PSDM
           AND A.PGSSRQ <> 0
           AND A.PGSSRQ IS NOT NULL)
   AND NOT EXISTS (SELECT 1
          FROM INFO.TZQ_KZZ B --过滤债券上市后的持仓
         WHERE F_GET_JYR_DATE(B.SSRQ, -1) <= ${hivevar:RQ}
           AND T.JYS = CASE
                   WHEN SUBSTR(B.GPDM, 1, 1) = '6' THEN
                    '2'
                   WHEN SUBSTR(B.GPDM, 1, 1) IN ('0', '3') THEN
                    '1'
                   ELSE
                    ''
               END
           AND T.ZQDM = B.RGDM
           AND B.SSRQ <> 0
           AND B.SSRQ IS NOT NULL)
   AND NOT EXISTS (SELECT 1
          FROM DSC_BAS.T_QYDM C --过滤权益代码的持仓
         WHERE T.JYS = C.JYS
           AND T.ZQDM = C.QYDM);


-----STEP 4：将数据生成目标表[DSC_BAS.T_ZQYE]
INSERT OVERWRITE TABLE DSC_BAS.T_ZQYE
    (RQ,
     KHH,
     KHXM,
     YYB,
     JYS,
     GDH,
     ZQDM,
     XWDM,
     ZQMC,
     ZQLB,
     BZ,
     ZQSL,
     DJSL,
     FLTSL,
     WJSSL,
     ZXSZ,
     KCRQ,
     BDRQ,
     MRJE,
     MRSL,
     PGJE,
     PGSL,
     MCJE,
     MCSL,
     HLJE,
     SGSL,
     JYFY,
     CCCB,
     LJYK,
     ZY,
     GT_ZXSZ,
     ZQPZ)
    SELECT RQ,
           KHH,
           KHXM,
           YYB,
           JYS,
           GDH,
           ZQDM,
           XWDM,
           ZQMC,
           ZQLB,
           BZ,
           ZQSL,
           DJSL,
           FLTSL,
           WJSSL,
           ZXSZ,
           KCRQ,
           BDRQ,
           MRJE,
           MRSL,
           PGJE,
           PGSL,
           MCJE,
           MCSL,
           HLJE,
           SGSL,
           JYFY,
           CCCB,
           LJYK,
           ZY,
           GT_ZXSZ,
           ZQPZ
      FROM (SELECT RQ,
                   KHH,
                   KHXM,
                   YYB,
                   JYS,
                   GDH,
                   ZQDM,
                   XWDM,
                   ZQMC,
                   ZQLB,
                   BZ,
                   ZQSL,
                   DJSL,
                   FLTSL,
                   WJSSL,
                   ZXSZ,
                   KCRQ,
                   BDRQ,
                   MRJE,
                   MRSL,
                   PGJE,
                   PGSL,
                   MCJE,
                   MCSL,
                   HLJE,
                   SGSL,
                   JYFY,
                   CCCB,
                   LJYK,
                   ZY,
                   GT_ZXSZ,
                   ZQPZ
              FROM TEMP.TEMP_T_ZQYE_04
            UNION ALL
            SELECT RQ,
                   KHH,
                   KHXM,
                   YYB,
                   JYS,
                   GDH,
                   ZQDM,
                   XWDM,
                   ZQMC,
                   ZQLB,
                   BZ,
                   ZQSL,
                   0       AS DJSL,
                   FLTSL,
                   0       AS WJSSL,
                   ZXSZ,
                   KCRQ,
                   BDRQ,
                   0       AS MRJE,
                   0       AS MRSL,
                   0       AS PGJE,
                   0       AS PGSL,
                   0       AS MCJE,
                   0       AS MCSL,
                   0       AS HLJE,
                   0       AS SGSL,
                   0       AS JYFY,
                   CCCB,
                   LJYK,
                   ZY,
                   GT_ZXSZ,
                   ZQPZ
              FROM DSC_BAS.T_QYDM_ZQYE_HIS
             WHERE RQ = ${hivevar:RQ}) T;

-----STEP 5：将(T-1日)的数据插入历史表
INSERT OVERWRITE TABLE  DSC_BAS.T_ZQYE_HIS
PARTITION
    (RQ = ${hivevar:RQ})
    (KHH,
     KHXM,
     YYB,
     JYS,
     GDH,
     ZQDM,
     XWDM,
     ZQMC,
     ZQLB,
     BZ,
     ZQSL,
     DJSL,
     FLTSL,
     WJSSL,
     ZXSZ,
     KCRQ,
     BDRQ,
     MRJE,
     MRSL,
     PGJE,
     PGSL,
     MCJE,
     MCSL,
     HLJE,
     SGSL,
     JYFY,
     CCCB,
     LJYK,
     ZY,
     GT_ZXSZ,
     ZQPZ) 
    SELECT KHH,
           KHXM,
           YYB,
           JYS,
           GDH,
           ZQDM,
           XWDM,
           ZQMC,
           ZQLB,
           BZ,
           ZQSL,
           DJSL,
           FLTSL,
           WJSSL,
           ZXSZ,
           KCRQ,
           BDRQ,
           MRJE,
           MRSL,
           PGJE,
           PGSL,
           MCJE,
           MCSL,
           HLJE,
           SGSL,
           JYFY,
           CCCB,
           LJYK,
           ZY,
           GT_ZXSZ,
           ZQPZ
      FROM DSC_BAS.T_ZQYE
     WHERE RQ = ${hivevar:RQ};


 -----STEP99:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_01;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_02;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_03;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQYE_04;

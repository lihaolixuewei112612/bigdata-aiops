/*
   @@name:资金存取款日统计
   @@componet_type: inceptor
   @@author:李伟
   @@version: V1.0.0
   @@update_time:2018-12-17
   @@comment:DSC_STAT 修改：插入分区表时去除插入列名列表中的分区字段
   ------------------------------------------------------------------------
      操作人      操作时间                      操作
      李伟        2018-12-17                    创建
      陈统        2019-07-31                    增加了ZHLB字段1 = 集中,2 = 两融,3 = 金融产品,5 = 个股
      李伟        2019-08-29                    修改：金融产品的资金转入转出
      李伟        2019-09-16                    修改：统计SJKM=105 利息收入
      李伟        2019-11-14                    修改：新增SJKM：103-服务佣金 111-其他收入  112-其他支出
*/
--STEP 0:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_STAT_ZJCQK_R_01;

--STEP 1:创建临时表
CREATE TABLE TEMP.TEMP_T_STAT_ZJCQK_R_01
AS
SELECT * FROM DSC_STAT.T_STAT_ZJCQK_R
WHERE 1 = 0;

-- 1.1:将集中的资金转入转出和利息收入按参数分类01、02、06统计
INSERT INTO TEMP.TEMP_T_STAT_ZJCQK_R_01
    (KHH, SJKM, YWKM, BZ, FSJE, ZHLB)
    SELECT A.KHH,
           CAST(CASE
                    WHEN C.PARAM_KIND = '01' THEN
                     '101'   --资金存入
                    WHEN C.PARAM_KIND = '02' THEN
                     '102'   --资金取出
                    WHEN C.PARAM_KIND = '03' THEN
                     '103'   --佣金收入
                    WHEN C.PARAM_KIND = '06' THEN
                     '105'   --利息收入
                    WHEN C.PARAM_KIND = '11' THEN
                     '111'   --其他收入
                    WHEN C.PARAM_KIND = '12' THEN
                     '112'   --其他支出
                END AS STRING) AS SJKM,
           A.YWKM,
           A.BZ,
           CAST(SUM(CASE
                        WHEN C.PARAM_KIND = '01' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '02' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '03' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '06' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '11' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '12' THEN
                         A.FCJE - A.SRJE
                        ELSE
                         0
                    END) AS DECIMAL(16, 2)) AS FSJE, -- 收入金额-付出金额 = 发生金额
           '1' AS ZHLB --集中交易账户
      FROM DSC_BAS.T_ZJMXLS_HIS A, DSC_STAT.T_PARAM_VALUE C
     WHERE A.RQ = ${hivevar:RQ}
       AND (A.SRJE <> 0 OR A.FCJE <> 0)
       AND A.YWKM = C.PARAM_VALUE
       AND C.PARAM_KIND IN ('01', '02', '03', '06', '11', '12')
     GROUP BY A.KHH, C.PARAM_KIND, A.YWKM, A.BZ;


-- 1.2:将两融的资金转入转出和利息收入按参数分类01、02、06统计
INSERT INTO TEMP.TEMP_T_STAT_ZJCQK_R_01
    (KHH, SJKM, YWKM, BZ, FSJE, ZHLB)
    SELECT A.KHH,
           CAST(CASE
                    WHEN C.PARAM_KIND = '01' THEN
                     '101' --资金存入
                    WHEN C.PARAM_KIND = '02' THEN
                     '102' --资金取出
                    WHEN C.PARAM_KIND = '03' THEN
                     '103' --佣金收入
                    WHEN C.PARAM_KIND = '06' THEN
                     '105' --利息收入
                    WHEN C.PARAM_KIND = '11' THEN
                     '111' --其他收入
                    WHEN C.PARAM_KIND = '12' THEN
                     '112' --其他支出
                END AS STRING) AS SJKM,
           A.YWKM,
           A.BZ,
           CAST(SUM(CASE
                        WHEN C.PARAM_KIND = '01' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '02' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '03' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '06' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '11' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '12' THEN
                         A.FCJE - A.SRJE
                        ELSE
                         0
                    END) AS DECIMAL(16, 2)) AS FSJE, -- 收入金额-付出金额 = 发生金额
           '2' AS ZHLB --融资融券账户
      FROM DSC_BAS.T_XY_ZJMXLS_HIS A, DSC_STAT.T_PARAM_VALUE C
     WHERE A.RQ = ${hivevar:RQ}
       AND (A.SRJE <> 0 OR A.FCJE <> 0)
       AND A.YWKM = C.PARAM_VALUE
       AND C.PARAM_KIND IN ('01', '02', '03', '06', '11', '12')
     GROUP BY A.KHH, C.PARAM_KIND, A.YWKM, A.BZ;


-- 1.3:将期权的资金转入转出和利息收入按参数分类01、02、06统计
INSERT INTO TEMP.TEMP_T_STAT_ZJCQK_R_01
    (KHH, SJKM, YWKM, BZ, FSJE, ZHLB)
    SELECT A.KHH,
           CAST(CASE
                    WHEN C.PARAM_KIND = '01' THEN
                     '101' --资金存入
                    WHEN C.PARAM_KIND = '02' THEN
                     '102' --资金取出
                    WHEN C.PARAM_KIND = '03' THEN
                     '103' --佣金收入
                    WHEN C.PARAM_KIND = '06' THEN
                     '105' --利息收入
                    WHEN C.PARAM_KIND = '11' THEN
                     '111' --其他收入
                    WHEN C.PARAM_KIND = '12' THEN
                     '112' --其他支出
                END AS STRING) AS SJKM,
           A.YWKM,
           A.BZ,
           CAST(SUM(CASE
                        WHEN C.PARAM_KIND = '01' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '02' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '03' THEN
                         A.FCJE - A.SRJE
                        WHEN C.PARAM_KIND = '06' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '11' THEN
                         A.SRJE - A.FCJE
                        WHEN C.PARAM_KIND = '12' THEN
                         A.FCJE - A.SRJE
                        ELSE
                         0
                    END) AS DECIMAL(16, 2)) AS FSJE, -- 收入金额-付出金额 = 发生金额
           '5' AS ZHLB --股票期权账户
      FROM DSC_BAS.T_SO_ZJMXLS_HIS A, DSC_STAT.T_PARAM_VALUE C
     WHERE A.RQ = ${hivevar:RQ}
       AND (A.SRJE <> 0 OR A.FCJE <> 0)
       AND A.YWKM = C.PARAM_VALUE
       AND C.PARAM_KIND IN ('01', '02', '03', '06', '11', '12')
     GROUP BY A.KHH, C.PARAM_KIND, A.YWKM, A.BZ;


--STEP 2：将源表[DSC_BAS.T_ZJMXLS_HIS]的存取款数据统计到表[DSC_STAT.T_STAT_ZJCQK_R]中
INSERT OVERWRITE TABLE DSC_STAT.T_STAT_ZJCQK_R PARTITION
    (RQ = ${hivevar:RQ})
    (KHH, YYB, GRJG, SJKM, YWKM, BZ, FSJE, ZHLB)
    SELECT T.KHH, KH.YYB, KH.GRJG, T.SJKM, T.YWKM, T.BZ, T.FSJE, T.ZHLB
      FROM TEMP.TEMP_T_STAT_ZJCQK_R_01 T
      JOIN DSC_BAS.T_KHXX_JJYW KH
        ON T.KHH = KH.KHH;

--STEP 99:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_STAT_ZJCQK_R_01;

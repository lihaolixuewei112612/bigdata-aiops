/*
	@@name:证券转入转出日统计
	@@componet_type: impala
	@@author:李伟
	@@version: V1.0.0
    @@update_time:2018-12-17
	@@comment:DSC_STAT 修改：插入分区表时去除插入列名列表中的分区字段

      ------------------------------------------------------------------------
       操作人      操作时间                   操作
       李伟      2018-12-17                	  创建
	   陈统		 2019-07-30					  修改转入转出证券类别 未在限定交易类别和成交编号的 存取方向(CQFX) 设定为NULL
	   陈统		 2019-07-31					  增加了ZHLB字段1 = 集中 2 = 两融 5 = 个股,并且根据ZHLB 判断CQFX
	   陈统		 2019-08-02					  对类别31根据YSSL判断 存取方向
	   陈统		 2019-08-08					  增加 ZHLB字段 3 = 金融产品
	   李伟      2019-08-08                   修改：添加证券数量>0的限制，目前发现交易类别为9 证券代码799999  登记指定 证券数量为0 并不会有市值变化
       李伟      2019-08-14                   修改：交易类别12 债券回售 暂时无法明确获取每支债券回售资金到账日期，暂时将债券回售处理成市值转出和资金转入
       李伟      2019-08-26                   修改：期权账户摘牌G且CJBH=行权注销统计到市值转入转出
	   李伟      2019-09-03                   修改：金融产品交割表中jslx='1' 存管资金结算，jslx='2' 理财资金结算统计到金融产品市值转出
	   李伟      2019-09-05                   修改：金融产品强行调增和强行调增处理成产品份额的转出转出
*/

set character.literal.as.string=true;

-- 定义变量
SET hivevar:VN_T_DATE = F_GET_JYR_DATE(${hivevar:RQ},-1);

--STEP 0:删除临时表
DROP TABLE IF EXISTS TEMP.T_STAT_ZQZRZC_R_01;

--STEP 0.1: 创建临时表
CREATE TABLE TEMP.T_STAT_ZQZRZC_R_01
AS
SELECT * FROM DSC_STAT.T_STAT_ZQZRZC_R
WHERE 1 = 0;

---------------------------集中交易-----------------------------------
--STEP 1: 将源表[DSC_BAS.T_JGMXLS_HIS]的证券转入转出数据统计到表TEMP.T_STAT_ZQZRZC_R_01中
INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.ZQDM,
           T.ZQLB,
           T.JYLB,
           T.BZ,
           T.CQFX,
           CAST(SUM(T.CJSL) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(SUM(T.CJJE) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('1' AS STRING) AS ZHLB --集中
      FROM (SELECT KHH,
                   YYB,
                   ZQDM,
                   ZQLB,
                   JYLB,
                   BZ,
                   CJBH,
                   CJSL,
                   CJJE,
                   CJRQ,
                   YSSL,
                   CAST(CASE -- 31 协议转让 根据应收数量判断转入转出
                       WHEN A.JYLB IN ('31') AND A.YSSL < 0 THEN
                        '2'
                       WHEN A.JYLB IN ('31') AND A.YSSL > 0 THEN
                        '1'
                       WHEN B.PARAM_KIND = '04' THEN
                        '1'
                       WHEN B.PARAM_KIND = '05' THEN
                        '2'
                       ELSE
                        '1'
                   END  AS STRING) CQFX -- 存取方向 1-证券转入，2-证券转出
              FROM DSC_BAS.T_JGMXLS_HIS A, DSC_STAT.T_PARAM_VALUE B
             WHERE A.CJRQ = ${hivevar:RQ}
               AND A.CJSL > 0
               AND NOT (A.YSJE > 0 AND A.YSSL = 0 AND A.CJSL = 0 AND JYLB = '12')  --过滤 回售资金
               AND B.PARAM_KIND IN ('04', '05') --04:市值转入 05:市值转出
               AND A.JYLB = B.PARAM_VALUE) T,
           DSC_BAS.T_KHXX_JJYW T1
     WHERE T.KHH = T1.KHH
     GROUP BY T.KHH, T.YYB, T1.GRJG, T.ZQDM, T.ZQLB, T.JYLB, T.BZ, T.CQFX;

INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.ZQDM,
           T.ZQLB,
           T.JYLB,
           T.BZ,
           T.CQFX,
           CAST(SUM(T.CJSL) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(SUM(T.CJJE) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('1' AS STRING) AS ZHLB --集中
      FROM (SELECT A.KHH,
                   A.YYB,
                   A.ZQDM,
                   A.ZQLB,
                   A.JYLB,
                   A.BZ,
                   A.CJBH,
                   ABS(A.YSSL) AS CJSL,
                   (ABS(A.YSSL) * (CASE
                    WHEN NVL(HQ.ZXJ,0) = 0 THEN
                    HQ.ZSP
                    ELSE
                    HQ.ZXJ
                    END)) AS CJJE,
                   A.CJRQ,
                   A.YSSL,
                   CAST(CASE
                       WHEN B.PARAM_KIND = '04' THEN
                        '1'
                       WHEN B.PARAM_KIND = '05' THEN
                        '2'
                       ELSE
                        '1'
                   END  AS STRING) CQFX -- 存取方向 1-证券转入，2-证券转出
              FROM DSC_BAS.T_JGMXLS_HIS A, DSC_STAT.T_PARAM_VALUE B
              ,(SELECT JYS, ZQDM, ZXJ, ZSP, RQ FROM DSC_BAS.T_ZQHQ_XZ_HIS WHERE RQ = ${hivevar:RQ}) HQ
             WHERE A.CJRQ = ${hivevar:RQ}
               AND A.JYLB = '21'
               AND A.CJBH = '非流通股转出'
               AND A.CJSL = 0
               AND A.YSSL <> 0
               AND B.PARAM_KIND IN ('04', '05') --04:市值转入 05:市值转出
               AND A.JYLB = B.PARAM_VALUE
               AND A.JYS = HQ.JYS
               AND A.ZQDM = HQ.ZQDM
               ) T,
           DSC_BAS.T_KHXX_JJYW T1
     WHERE T.KHH = T1.KHH
     GROUP BY T.KHH, T.YYB, T1.GRJG, T.ZQDM, T.ZQLB, T.JYLB, T.BZ, T.CQFX;

----------------------------------融资融券-----------------------------
--STEP 2: 将源表[DSC_BAS.T_XY_JGMXLS_HIS]的证券转入转出数据统计到表TEMP.T_STAT_ZQZRZC_R_01中
INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.ZQDM,
           T.ZQLB,
           T.JYLB,
           T.BZ,
           T.CQFX,
           CAST(SUM(T.CJSL) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(SUM(T.CJJE) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('2' AS STRING) AS ZHLB --两融
      FROM (SELECT KHH,
                   YYB,
                   ZQDM,
                   ZQLB,
                   JYLB,
                   BZ,
                   CJBH,
                   CJSL,
                   CJJE,
                   CJRQ,
                   YSSL,
                   CAST(CASE -- 31 协议转让 根据应收数量判断转入转出
                       WHEN A.JYLB IN ('31') AND A.YSSL < 0 THEN
                        '2'
                       WHEN A.JYLB IN ('31') AND A.YSSL > 0 THEN
                        '1'
                       WHEN B.PARAM_KIND = '04' THEN
                        '1'
                       WHEN B.PARAM_KIND = '05' THEN
                        '2'
                       ELSE
                        '1'
                   END AS STRING) CQFX -- 存取方向 1-证券转入，2-证券转出
              FROM DSC_BAS.T_XY_JGMXLS_HIS A, DSC_STAT.T_PARAM_VALUE B
             WHERE A.CJRQ = ${hivevar:RQ}
               AND A.CJSL > 0
               AND B.PARAM_KIND IN ('04', '05') --04:市值转入 05:市值转出
               AND A.JYLB = B.PARAM_VALUE) T,
           DSC_BAS.T_KHXX_JJYW T1
     WHERE T.KHH = T1.KHH
     GROUP BY T.KHH, T.YYB, T1.GRJG, T.ZQDM, T.ZQLB, T.JYLB, T.BZ, T.CQFX;

INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.ZQDM,
           T.ZQLB,
           T.JYLB,
           T.BZ,
           T.CQFX,
           CAST(SUM(T.CJSL) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(SUM(T.CJJE) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('2' AS STRING) AS ZHLB --集中
      FROM (SELECT A.KHH,
                   A.YYB,
                   A.ZQDM,
                   A.ZQLB,
                   A.JYLB,
                   A.BZ,
                   A.CJBH,
                   ABS(A.YSSL) AS CJSL,
                   (ABS(A.YSSL) * (CASE
                    WHEN NVL(HQ.ZXJ,0) = 0 THEN
                    HQ.ZSP
                    ELSE
                    HQ.ZXJ
                    END)) AS CJJE,
                   A.CJRQ,
                   A.YSSL,
                   CAST(CASE
                       WHEN B.PARAM_KIND = '04' THEN
                        '1'
                       WHEN B.PARAM_KIND = '05' THEN
                        '2'
                       ELSE
                        '1'
                   END  AS STRING) CQFX -- 存取方向 1-证券转入，2-证券转出
              FROM DSC_BAS.T_XY_JGMXLS_HIS A, DSC_STAT.T_PARAM_VALUE B
              ,(SELECT JYS, ZQDM, ZXJ, ZSP, RQ FROM DSC_BAS.T_ZQHQ_XZ_HIS WHERE RQ = ${hivevar:RQ}) HQ

             WHERE A.CJRQ = ${hivevar:RQ}
               AND A.JYLB = '21'
               AND A.CJBH = '非流通股转出'
               AND A.CJSL = 0
               AND A.YSSL <> 0
               AND B.PARAM_KIND IN ('04', '05') --04:市值转入 05:市值转出
               AND A.JYLB = B.PARAM_VALUE
               AND A.JYS = HQ.JYS
               AND A.ZQDM = HQ.ZQDM
               ) T,
           DSC_BAS.T_KHXX_JJYW T1
     WHERE T.KHH = T1.KHH
     GROUP BY T.KHH, T.YYB, T1.GRJG, T.ZQDM, T.ZQLB, T.JYLB, T.BZ, T.CQFX;
-------------------------------股票期权---------------------------
--STEP 3:将源表[DSC_BAS.T_SO_JGMXLS_HIS]的证券转入转出数据统计到表TEMP.T_STAT_ZQZRZC_R_01中
INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.HYDM AS ZQDM,
           T.ZQLX AS ZQLB,
           T.MMFX AS JYLB,
           T.BZ,
           CAST(CASE
               WHEN T.MMFX = '2' THEN
                '1'
               WHEN T.MMFX = '1' THEN
                '2'
           END AS STRING) AS CQFX, -- 存取方向 1-证券转入，2-证券转出
           CAST(SUM(T.CJSL) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(ROUND((SUM(CASE
                               WHEN T.KPBZ = 'G' THEN
                                (CASE
                                    WHEN T.YSSL < 0 AND HQ.ZXJ != 0 THEN
                                     ABS(T.YSSL) * HQ.JYDW * (CASE
                                                                  WHEN HQ.ZXJ > 0 THEN
                                                                   HQ.ZXJ
                                                                  ELSE
                                                                   HQ.ZSP
                                                              END)
                                    ELSE
                                     NVL(P.ZXSZ, 0)
                                END)
                               WHEN T.KPBZ = 'E' THEN
                                (CASE
                                    WHEN T.YSJE != 0 THEN
                                     ABS(YSJE)
                                    WHEN T.YSSL < 0 AND HQ.ZXJ != 0 THEN
                                     ABS(T.YSSL) * HQ.JYDW * (CASE
                                                                  WHEN HQ.ZXJ > 0 THEN
                                                                   HQ.ZXJ
                                                                  ELSE
                                                                   HQ.ZSP
                                                              END)
                                    ELSE
                                     0
                                END)
                               ELSE
                                0
                           END)),
                      2) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('5' AS STRING) AS ZHLB
      FROM DSC_BAS.T_SO_JGMXLS_HIS T
      JOIN DSC_BAS.T_KHXX_JJYW T1
        ON T.KHH = T1.KHH
      LEFT JOIN DSC_BAS.T_SO_HYHQ_HIS HQ
        ON (T.JYS = HQ.JYS AND T.HYDM = HQ.HYDM AND RQ = ${hivevar:RQ})
      LEFT JOIN DSC_BAS.T_SO_ZQYE_HIS P
        ON (T.KHH = P.KHH AND T.GDH = P.GDH AND T.ZZHBM = P.ZZHBM AND T.JYS = P.JYS AND
           T.HYDM = P.HYDM AND P.RQ = ${hivevar:VN_T_DATE})
     WHERE T.CJRQ = ${hivevar:RQ}
       AND ((T.KPBZ = 'G' AND T.CJBH IN ('到期注销','行权注销')) OR
           (T.KPBZ = 'E' AND T.CJBH IN ('行权交收')))
     GROUP BY T.KHH,
              T.YYB,
              T1.GRJG,
              T.HYDM,
              T.ZQLX,
              T.MMFX,
              T.BZ,
              CASE
                  WHEN T.MMFX = '2' THEN
                   '1'
                  WHEN T.MMFX = '1' THEN
                   '2'
              END;

--------------------------------------金融产品-------------------------------------
--STEP 4: 将源表[DSC_BAS.T_FP_JGMXLS_HIS]的证券转入转出数据统计到表TEMP.T_STAT_ZQZRZC_R_01中
-- 150 基金清盘  198 快过确认  124 赎回 143 红利发放-现金分红 算市值转出,034 内部过户转入
INSERT INTO TEMP.T_STAT_ZQZRZC_R_01
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T1.GRJG,
           T.CPDM AS ZQDM,
           T.CPFL AS ZQLB,
           T.YWDM AS JYLB,
           T.BZ,
           T.CQFX, -- 存取方向 1-证券转入，2-证券转出
           CAST(SUM(T.QRFE) AS DECIMAL(22, 2)) AS ZQSL,
           CAST(SUM(T.QRJE) AS DECIMAL(22, 2)) AS ZQSZ,
           CAST('3' AS STRING) AS ZHLB --金融产品账户
      FROM (SELECT A.KHH,
                   A.YYB,
                   A.YWDM,
                   A.CPDM,
                   A.CPFL,
                   A.BZ,
                   A.QRFE,
                   A.QRJE,
                   A.QRRQ,
                   CAST(CASE
                       WHEN (B.PARAM_KIND = '07') THEN
                        '1'
                       WHEN (B.PARAM_KIND = '08') THEN
                        '2'
                       ELSE
                        '1'
                   END AS STRING) AS CQFX,
                   A.JSLX
              FROM DSC_BAS.T_FP_JGMXLS_HIS A, DSC_STAT.T_PARAM_VALUE B
             WHERE QRRQ = ${hivevar:RQ}
               AND A.YWDM = B.PARAM_VALUE
               AND B.PARAM_KIND IN ('07', '08') --07 金融产品份额转入  08 金融产品份额转出
            ) T,
           DSC_BAS.T_KHXX_JJYW T1
     WHERE T.KHH = T1.KHH
       AND NOT ((T.JSLX = '1' AND YWDM IN ('124', '150')) OR   --150 基金清盘  124 赎回
            (T.YWDM = '198' AND T.CPDM != '000709') OR
            (T.YWDM = '143' AND T.QRJE = 0 AND T.QRFE > 0) OR   -- 143 红利发放-红利转投
            (T.YWDM = '143' AND T.JSLX = '1' AND T.QRJE > 0 AND T.QRFE = 0)) -- 143 红利发放-现金分红
     GROUP BY T.KHH, T.YYB, T1.GRJG, T.CPDM, T.CPFL, T.YWDM, T.BZ, T.CQFX;


--STEP 5：将临时表证券市值转入转出数据统计到表[DSC_STAT.T_STAT_ZQZRZC_R]中
INSERT OVERWRITE TABLE DSC_STAT.T_STAT_ZQZRZC_R
PARTITION
    (RQ = ${hivevar:RQ})
    (KHH, YYB, GRJG, ZQDM, ZQLB, JYLB, BZ, CQFX, ZQSL, ZQSZ, ZHLB)
    SELECT T.KHH,
           T.YYB,
           T.GRJG,
           T.ZQDM,
           T.ZQLB,
           T.JYLB,
           T.BZ,
           T.CQFX,
           T.ZQSL,
           T.ZQSZ,
           T.ZHLB
      FROM TEMP.T_STAT_ZQZRZC_R_01 T;

--STEP 99:删除临时表
DROP TABLE IF EXISTS TEMP.T_STAT_ZQZRZC_R_01;

CREATE OR REPLACE PROCEDURE cust.p_ids_xy_cost_calcution(
--输入变量
I_RQ IN INT,
I_KHH IN STRING
)
IS
/******************************************************************
  *文件名称：CUST.P_IDS_XY_COST_CALCUTION
  *项目名称：IDS计算
  *文件说明：融资融券-成本盈亏修正处理

  创建人：燕居庆
  功能说明：融资融券-成本盈亏修正处理

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/19           创建
  王睿驹        v1.0.1        2019/8/22        根据java代码修改
  王睿驹        v1.0.2        2019/9/13        根据java代码修改
  燕居庆        v1.0.3        2019/9/25        对标java-ids 5028版本
                                                1.生成当日买入/卖出相关数据 增加cjrq <= jsrq 条件
                                                2.市价业务 调整mrje/mrsl/mcje/mcsl
                                                3.增加基金赎回，写入成本计算
  燕居庆        v1.0.4        2019/9/27       对标java-ids 5080版本
                                                1.生成当日买入卖出相关调整
  燕居庆        v1.0.5        2019/11/8       对标java-ids 5435版本
                                                1.增加交易类别 78盘后买入、79盘后卖出处理
 燕居庆        v1.0.6        2019/11/29      增加JYS=2 zqlb = L0处理，科创板基金
 燕居庆         v1.0.7        2020/05/07      修改：针对深圳配股上市，计算成本价为数量*配股价
  邱建锋		   v1.0.9		  2020/07/24	  1、基金赎回后成本未降低，调整处理逻辑
											  2、剔除送股计入买入金额
*******************************************************************/
l_sqlBuf STRING;    --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING;
l_sqlWhereLastDay STRING;
l_lastDay INT;
l_last2Days INT;
l_nextDay INT;
l_sql STRING;
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
l_columns_k STRING;
BEGIN
  -- 获取上日交易日
  BEGIN
    SELECT f_get_jyr_date(I_RQ, -1) INTO l_lastDay FROM system.dual;
    SELECT f_get_jyr_date(I_RQ, -2) INTO l_last2Days FROM system.dual;
    SELECT f_get_jyr_date(I_RQ, 1) INTO l_nextDay FROM system.dual;
  END;

  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay;
  ELSE
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
  END IF;

  --0.加载源数据
  BEGIN
    --0.1 获取当日交割清算数据
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_XY_JGMXLS_HIS_QS WHERE CJRQ = ' || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    --0.2 获取上日成本盈亏
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkCbjsPre1', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_XY_ZQYE_CBJS WHERE RQ = ' || l_sqlWhereLastDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;


  BEGIN
    --0.4 获取当日日持仓
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZqyeDR', I_KHH);
    l_sqlBuf := "select 0 tzzh_id,rq,khh,jys,gdh,zqdm,zqlb,bz,zqsl,null as kcrq,null as fltsl,zxsz,
                 null as cccb,null as cbj,null as ljyk,null as tbcccb,null as tbcbj,null as dryk, ZQSL AS ZQSL_ORIG
                 from dsc_bas.t_xy_zqye_his t where t.zqlb <> 'F8' and t.zqsl>0 AND T.ZQDM NOT LIKE 'SHRQ%' AND T.ZQDM NOT LIKE 'SZRQ% ' and zqdm not in  ('888880','900000') and rq= "|| l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    --0.4 获取上日持仓
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZqyeSR', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_XY_ZQYE_HIS WHERE RQ = ' || l_sqlWhereLastDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    --0.5 获取待交收交收当天数据
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkDjsDRjs', I_KHH);
    l_sqlBuf := 'SELECT * FROM dsc_bas.T_XY_DJSQSZL_HIS WHERE SETTLE_DATE_2 = ' || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    --0.6 获取待交收清算数据
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkDjsT', I_KHH);
    l_sqlBuf := 'SELECT * FROM dsc_bas.T_XY_DJSQSZL_HIS WHERE SETTLE_DATE_2 > ' || l_lastDay || ' AND SETTLE_DATE_2 <= ' || I_RQ || IF(I_KHH IS NULL, '', ' AND KHH = ' || I_KHH);

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  --1.生成货币基金申购赎回流水数据，并更新到T_XY_ZQYE_CBJS_HBJJSGSH表
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHResult', I_KHH);
    l_sqlBuf := "SELECT T.TZZH_ID,
                         T.KHH,
                         T.GDH,
                         T.JYS,
                         T.ZQDM,
                         T.SGSHDM,
                         NULL AS ZQLB,
                         T.BZ,
                         CAST(T.DRSGJE AS DECIMAL(16, 2)) AS DRSGJE,
                         JSRQ AS DZRQ_SG,
                         CAST(T.DRSGSL AS INT) AS DRSGSL,
                         JSRQ AS WTRQ_SH,
                         CAST(T.DRSHSL AS INT) AS DRSHSL,
                         CAST(T.DRSHJE AS DECIMAL(16, 2)) AS DRSHJE," ||I_RQ|| " as rq
                    FROM (SELECT JG.TZZH_ID,
                                 JG.KHH,
                                 JG.GDH,
                                 JG.JYS,
                                 D.JJDM AS ZQDM,
                                 D.SGSHDM,
                                 JG.BZ,
                                 CASE WHEN JYLB='29' AND YSJE<0 AND CJRQ<JSRQ THEN JSRQ ELSE CJRQ END AS JSRQ,
                                 SUM(CASE
                                       WHEN (JYLB = '30' AND YSJE > 0) OR (JYLB = '29' AND YSJE > 0) THEN
                                        ABS(YSJE)
                                       ELSE
                                        0
                                     END) AS DRSHJE,
                                 SUM(CASE
                                       WHEN JYLB = '30' AND YSSL < 0 AND SEQNO!=0 THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS DRSHSL,
                                 SUM(CASE
                                       WHEN JYLB = '29' AND YSJE < 0 THEN
                                        ABS(YSJE)
                                       ELSE
                                        0
                                     END) AS DRSGJE,
                                 SUM(CASE
                                       WHEN JYLB = '29' AND YSSL > 0 THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS DRSGSL
                            FROM "|| F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH) ||" JG, CUST.T_HBJJXX  D
                           WHERE JG.JYS = D.JYS
                             AND (JG.ZQDM = D.JJDM OR JG.ZQDM = D.SGSHDM)
                             AND JG.JYLB IN ('29', '30')
                             AND NOT (ZQLB IN ('E0','E4') AND JYLB = '29' AND SEQNO = 0)
                             AND D.JYS = '2'
                           GROUP BY JG.TZZH_ID,
                                    JG.KHH,
                                    JG.GDH,
                                    JG.JYS,
                                    D.JJDM,
                                    D.SGSHDM,
                                    JG.BZ,
                                    CASE WHEN JYLB = '29' AND YSJE < 0 AND CJRQ<JSRQ THEN JSRQ ELSE CJRQ END) T";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_XY_ZQYE_CBJS_HBJJSGSH', I_RQ, I_KHH);
  END;

  --1.1 货币基金申购
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsg', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_XY_ZQYE_CBJS_HBJJSGSH WHERE DZRQ_SG = ' || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  --1.2 货币基金申购赎回
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsh', I_KHH);
    l_sqlBuf := 'select * from CUST.T_XY_ZQYE_CBJS_HBJJSGSH WHERE WTRQ_SH = ' || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
      l_sqlBuf := "SELECT TZZH_ID,
                             DZRQ_SG AS RQ,
                             KHH,
                             JYS,
                             GDH,
                             ZQDM,
                             BZ,
                             SUM(T.DRSGSL) AS DRYSSL,
                             SUM(T.DRSGJE) AS DRMRJE,
                             SUM(T.DRSGSL) AS DRMRSL,
                             0 AS DRMCJE,
                             0 AS DRMCSL,
                             0 AS DRJYFY,
                             0 AS DRSHDQSL,
                             0 AS DRSHDQJE,
                             0 AS DRSHSYSL,
                             0 AS SRSHSYSL,
                             0 AS DRHBCFJE_ZR,
                             0 AS DRHBCFSL_ZR,
                             0 AS DRHBCFJE_ZC,
                             0 AS DRHBCFSL_ZC,
                             0 AS DRSHSL_ETFHB,
                             SUM(T.DRSGSL) AS DRSGSL_ETFHB,
                             0 AS DRSHJE_SJ_ETFHB,
                             SUM(T.DRSGJE) AS DRSGJE_SJ_ETFHB,
                             0 AS DRPGJE,
                             0 AS DRPGSL,
                             0 AS drshje_ss_etfhb,
                             SUM(T.DRSGJE) AS DRSGJE_SF_ETFHB
                        FROM "|| F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsg', I_KHH) ||" T
                       GROUP BY TZZH_ID, DZRQ_SG, KHH, JYS, GDH, ZQDM, BZ
                     UNION ALL
                     SELECT  TZZH_ID,
                             WTRQ_SH AS RQ,
                             KHH,
                             JYS,
                             GDH,
                             ZQDM,
                             BZ,
                             -SUM(T.DRSHSL) AS DRYSSL,
                             0 AS DRMRJE,
                             0 AS DRMRSL,
                             SUM(DRSHJE) AS DRMCJE,
                             SUM(DRSHSL) AS DRMCSL,
                             0 AS DRJYFY,
                             0 AS DRSHDQSL,
                             0 AS DRSHDQJE,
                             0 AS DRSHSYSL,
                             0 AS SRSHSYSL,
                             0 AS DRHBCFJE_ZR,
                             0 AS DRHBCFSL_ZR,
                             0 AS DRHBCFJE_ZC,
                             0 AS DRHBCFSL_ZC,
                             SUM(T.DRSHSL) AS DRSHSL_ETFHB,
                             0 AS DRSGSL_ETFHB,
                             SUM(T.DRSHJE) AS DRSHJE_SJ_ETFHB,
                             0 AS DRSGJE_SJ_ETFHB,
                             0 AS DRPGJE,
                             0 AS DRPGSL,
                             SUM(T.DRSHJE) AS drshje_ss_etfhb,
                             0 AS DRSGJE
                       FROM "|| F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsh', I_KHH) ||" T
                      GROUP BY TZZH_ID, WTRQ_SH, KHH, JYS, GDH, ZQDM, BZ";
        l_tableName := F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsgRe', I_KHH);
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    END;

  --2 生成当日买入/卖出相关数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkCbjsDR', I_KHH);
    l_sqlBuf := "SELECT A.TZZH_ID,
                        A.KHH,
                        A.JYS,
                        A.GDH,
                        A.ZQDM,
                        A.BZ,
                        CAST(A.YSSL AS DECIMAL(22,2)) AS DRYSSL,
                        CAST(A.MRJE AS DECIMAL(16,2)) AS DRMRJE,
                        CAST(A.MRSL AS DECIMAL(22,2)) AS DRMRSL,
                        CAST(A.MCJE AS DECIMAL(16,2)) AS DRMCJE,
                        CAST(A.MCSL AS DECIMAL(22,2)) AS DRMCSL,
                        CAST(A.JYFY AS DECIMAL(16,2)) AS DRJYFY,
                        CAST(A.DRSHDQSL AS DECIMAL(16,2)) AS DRSHDQSL,
                        CAST(A.DRSHDQJE AS DECIMAL(16,2)) AS DRSHDQJE,
                        CAST(A.DRSHSYSL AS DECIMAL(16,2)) AS DRSHSYSL,
                        0 AS SRSHSYSL,
                        0 AS DRHBCFJE_ZR,
                        0 AS DRHBCFSL_ZR,
                        0 AS DRHBCFJE_ZC,
                        0 AS DRHBCFSL_ZC,
                        0 AS DRSHSL_ETFHB,
                        0 AS DRSGSL_ETFHB,
                        0 AS DRSHJE_SJ_ETFHB,
                        0 AS DRSGJE_SJ_ETFHB,
                        0 AS DRPGJE,
                        0 AS DRPGSL,
                        0 AS drshje_ss_etfhb,
                        0 AS DRSGJE_SF_ETFHB
                    FROM (SELECT TZZH_ID,
                                 KHH,
                                 GDH,
                                 JYS,
                                 ZQDM,
                                 BZ,
                                 SUM(YSSL) AS YSSL,
                                 SUM(CASE
                                       WHEN JYLB IN ('1', '59', '61', '41', '80', '83', '93', '94','3', '78') OR"||-- 20200724 剔除送股计入买入金额
									   "
										    (JYLB = '49' AND YSJE<0) OR
                                            (JYLB = '42' AND JSRQ <> 0) OR (JYLB='11' AND YSSL>0) THEN
                                        ABS(YSJE)
                                       ELSE
                                        0
                                     END) AS MRJE,
                                 SUM(CASE
                                       WHEN JYLB IN
                                            ('1', '16', '59', '61', '41', '80', '83','3', '78') OR
                                            (JYLB = '42' AND JSRQ <> 0) OR (JYLB='11' AND YSSL>0) THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS MRSL,
                                 SUM(CASE
                                       WHEN ZQLB = 'L0' and JYLB = '81' THEN  /*2卖出,6分红,17兑付,22兑息,43赎回,45分红,71融资强平,81退保,79盘后卖出*/
                                        0
                                       WHEN JYLB IN
                                            ('2', '6', '17', '22', '45', '60', '62', '71', '81', '76', '79') OR
                                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '2') OR (JYLB='11' AND YSSL<0) OR "|| --20200724 基金赎回后成本未降低，调整处理逻辑
                            "                (JYLB = '43' AND JSRQ <> 0 AND JYS = '1') OR
                                            (JYLB = '43' AND JYS = '2' AND JSRQ <> 0 AND CJRQ >= JSRQ AND ZQLB='J0') OR (JYLB='49' AND YSJE>0) THEN
                                        ABS(YSJE)
                                       ELSE
                                        0
                                     END) AS MCJE,
                                 SUM(CASE
                                       WHEN JYLB = '43' AND ABS(YSJE) = 0 THEN
                                        0
                                       WHEN JYLB IN
                                            ('2', '6', '17', '22', '45', '60', '62', '71', '81', '76', '79') OR
                                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '2') OR (JYLB='11' AND YSSL<0) OR " ||--20200724 基金赎回后成本未降低，调整处理逻辑
                                    "        (JYLB = '43' AND JSRQ <> 0
											AND CJRQ <= JSRQ AND JYS = '1') OR
                                            (JYLB = '43' AND JYS = '2' AND JSRQ <> 0 AND CJRQ >= JSRQ AND ZQLB='J0') THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS MCSL,
                                 SUM(CASE
                                       WHEN JYLB = '43' AND (CJRQ > JSRQ AND JSRQ <> 0) THEN
                                        0
                                       ELSE
                                        S1 + S2 + S3 + S4 + S5 + S6
                                     END) AS JYFY,
                                 SUM(CASE
                                       WHEN (JYLB = '43'  AND CJRQ> JSRQ AND JSRQ <> 0) THEN
                                        ABS(YSJE)
                                       ELSE
                                        0
                                     END) AS DRSHDQJE,
                                 SUM(CASE
                                       WHEN (JYLB = '43' AND CJRQ > JSRQ AND JSRQ <> 0) THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS DRSHDQSL,
                                 SUM(CASE
                                       WHEN (JYLB = '43' AND CJRQ <= JSRQ) THEN
                                        ABS(YSSL)
                                       ELSE
                                        0
                                     END) AS DRSHSYSL
                            FROM "|| F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH) ||"
                           WHERE (YSSL <> 0 OR YSJE <> 0)
                           GROUP BY TZZH_ID, KHH, GDH, JYS, ZQDM, BZ) A";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

  BEGIN
     -- 获取临时表字段，并拼接(column1, column2, column3....)
    l_tableArr := get_columns(l_tableName);
    l_columns := '( ';
    FOR indx IN l_tableArr.first() .. l_tableArr.last() LOOP
      IF indx = l_tableArr.last() THEN
        l_columns := l_columns || l_tableArr(indx) || ') ';
      ELSE
        l_columns := l_columns || l_tableArr(indx) || ',';
      END IF;
    END LOOP;
  END;



  --4 视作按市价的业务:指定撤指(9,10),转债转股(11),回售(12),赎回(13),转托管(7,15),ETF申赎(29,30),基金拆分合并(47,48)，非交易过户(31)
  BEGIN
    l_sqlBuf := "SELECT A.TZZH_ID,
                        A.KHH,
                        A.JYS,
                        A.GDH,
                        A.ZQDM,
                        A.BZ,
                        0 AS DRYSSL,
                        CAST(A.MRJE AS DECIMAL(16,2)) AS DRMRJE,
                        CAST(A.MRSL AS DECIMAL(22,2)) AS DRMRSL,
                        CAST(A.MCJE AS DECIMAL(16,2)) AS DRMCJE,
                        CAST(A.MCSL AS DECIMAL(22,2)) AS DRMCSL,
                        CAST(A.JYFY AS DECIMAL(16,2)) AS DRJYFY,
                        0 AS DRSHDQSL,
                        0 AS DRSHDQJE,
                        0 AS DRSHSYSL,
                        0 AS SRSHSYSL,
                        CAST(A.DRHBCFJE_ZR AS DECIMAL(16,2)) AS DRHBCFJE_ZR,
                        CAST(A.DRHBCFSL_ZR AS DECIMAL(16,2)) AS DRHBCFSL_ZR,
                        CAST(A.DRHBCFJE_ZC AS DECIMAL(16,2)) AS DRHBCFJE_ZC,
                        CAST(A.DRHBCFSL_ZC AS DECIMAL(16,2)) AS DRHBCFSL_ZC,
                        CAST(A.DRSHSL_ETFHB AS DECIMAL(16,2)) AS DRSHSL_ETFHB,
                        CAST(A.DRSGSL_ETFHB AS DECIMAL(16,2)) AS DRSGSL_ETFHB,
                        CAST(A.DRSHJE_SJ_ETFHB AS DECIMAL(16,2)) AS DRSHJE_SJ_ETFHB,
                        CAST(A.DRSGJE_SJ_ETFHB AS DECIMAL(16,2)) AS DRSGJE_SJ_ETFHB,
                        0 AS DRPGJE,
                        0 AS DRPGSL,
                        CAST(A.drshje_ss_etfhb AS DECIMAL(16,2)) AS drshje_ss_etfhb,
                        CAST(A.DRSGJE_SF_ETFHB AS DECIMAL(16,2)) AS DRSGJE_SF_ETFHB
                    FROM (SELECT B.TZZH_ID,
                                 B.KHH,
                                 B.GDH,
                                 B.JYS,
                                 B.ZQDM,
                                 B.BZ,
                                 ROUND(SUM(CASE
                                               WHEN B.JYLB='65' AND B.JYS='1' THEN B.CJJE
                                               WHEN B.JYLB='18' AND B.JYS='2' AND B.IS_PG=1 AND B.CJBH='上市流通' THEN B.CJSL*B.PGJG
                                               WHEN B.JYLB='18' AND B.JYS='1' AND B.IS_PG=1 AND B.CJBH='上市转入' THEN B.CJSL*B.PGJG
                                               WHEN B.JYLB='18' AND B.JYS='2' AND B.IS_KG=1 AND B.CJBH='上市流通' THEN 0
                                               WHEN B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '18' AND B.CJBH = '份额变动' THEN 0
                                               WHEN (JYLB IN ('15', '18', '47', '48') OR (JYLB IN ('29','30') AND IS_ETFHB=0)) AND YSSL > 0 THEN
                                                  ABS(B.YSSL * A.ZXJ)
                                               WHEN JYLB IN ('9','20','38') OR (JYLB IN ('31','65') AND YSSL>0) THEN ABS(B.CJJE)
                                               ELSE 0
                                           END),
                                       2) AS MRJE,
                                 SUM(CASE
                                         WHEN B.JYLB='65' AND B.JYS='1' THEN B.CJSL
                                         WHEN ((JYLB IN
                                          ('15', '18','47', '48')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) AND
                                            YSSL > 0 THEN
                                           ABS(B.YSSL)
                                        WHEN JYLB IN ('9','20','38') OR (JYLB IN ('31','65') AND YSSL>0) THEN ABS(B.CJSL)
                                         ELSE 0
                                     END) AS MRSL, " ||
                                 "                 ROUND(SUM(CASE" ||
                "                             WHEN (JYLB IN ('13', '47', '48') OR (JYLB = '19' AND ZQLB NOT IN ('A4', 'C4')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) " ||
                "                                   AND NOT (B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '19' AND B.CJBH = '份额变动') " ||
                "                                   AND NOT (JYLB='19' AND B.ZQLB='Z4' AND B.CJBH='调整变动')" ||
                "                                   AND NOT (JYLB='19' AND B.ZQLB='Z6' AND B.JYS='2' AND (B.CJBH='非交易过户' OR B.CJBH='no cjbh')) " ||
                "                                   AND YSSL < 0 THEN" ||
                "                              ABS(B.YSSL * A.ZXJ)" ||
                "                             WHEN JYLB IN ('7','10','21','15','37') OR (JYLB IN ('12','31','66') AND YSSL<0) THEN ABS(B.CJJE)" ||
                "                             WHEN JYLB IN ('31') AND B.CJJE=0 AND B.YSSL=0 AND B.YSJE>0 THEN B.YSJE" ||
                "                             ELSE 0" ||
                "                           END)," ||
                "                       2) AS MCJE," ||
                "                 SUM(CASE" ||
                "                       WHEN (JYLB IN ('13', '47', '48') OR (JYLB = '19' AND ZQLB NOT IN ('A4', 'C4')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) " ||
                "                            AND NOT (B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '19' AND B.CJBH = '份额变动') " ||
                "                            AND NOT (JYLB='19' AND B.ZQLB='Z6' AND B.JYS='2' AND (B.CJBH='非交易过户' OR B.CJBH='no cjbh'))" ||
                "                            AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       WHEN JYLB IN ('7','10','21','15','37') OR (JYLB IN ('12','31','66') AND YSSL<0) THEN ABS(B.CJSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MCSL," ||
                "                 SUM(S1 + S2 + S3 + S4 + S5 + S6) AS JYFY,
                                 ROUND(SUM(CASE
                                             WHEN ((JYLB IN ('18') AND CJBH = '拆分合并') OR
                                                  JYLB IN ('47', '48')) AND YSSL > 0 THEN
                                              ABS(B.YSSL * CASE
                                                    WHEN A.ZXJ > 0 THEN
                                                     A.ZXJ
                                                    ELSE
                                                     A.ZSP
                                                  END)
                                             ELSE
                                              0
                                           END),
                                       2) AS DRHBCFJE_ZR,
                                 SUM(CASE
                                       WHEN ((JYLB IN ('18') AND CJBH = '拆分合并') OR
                                            JYLB IN ('47', '48')) AND YSSL > 0 THEN
                                        ABS(B.YSSL)
                                       ELSE
                                        0
                                     END) AS DRHBCFSL_ZR,
                                 ROUND(SUM(CASE
                                             WHEN ((JYLB IN ('19') AND CJBH = '拆分合并') OR
                                                  JYLB IN ('47', '48')) AND YSSL < 0 THEN
                                              ABS(B.YSSL * CASE
                                                    WHEN A.ZXJ > 0 THEN
                                                     A.ZXJ
                                                    ELSE
                                                     A.ZSP
                                                  END)
                                             ELSE
                                              0
                                           END),
                                       2) AS DRHBCFJE_ZC,
                                 SUM(CASE
                                       WHEN ((JYLB IN ('19') AND CJBH = '拆分合并') OR
                                            JYLB IN ('47', '48')) AND YSSL < 0 THEN
                                        ABS(B.YSSL)
                                       ELSE
                                        0
                                     END) AS DRHBCFSL_ZC,
                                 SUM(CASE
                                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN
                                        ABS(B.YSSL)
                                       ELSE
                                        0
                                     END) AS DRSHSL_ETFHB,
                                 SUM(CASE
                                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN
                                        ABS(B.YSSL)
                                       ELSE
                                        0
                                     END) AS DRSGSL_ETFHB,
                                 SUM(CASE
                                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN
                                        ABS(B.YSSL * A.ZXJ)
                                       ELSE
                                        0
                                     END) AS DRSHJE_SS_ETFHB,
                                 SUM(CASE
                                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN
                                        ABS(B.YSSL * A.ZXJ)
                                       ELSE
                                        0
                                     END) AS DRSGJE_SF_ETFHB,
                                 SUM(CASE
                                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN
                                        ABS(B.YSSL * A.ZXJ)
                                       ELSE
                                        0
                                     END) AS DRSHJE_SJ_ETFHB,
                                 SUM(CASE
                                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN
                                        ABS(B.YSSL * A.ZXJ)
                                       ELSE
                                        0
                                     END) AS DRSGJE_SJ_ETFHB
                            FROM (SELECT * FROM  DSC_BAS.T_ZQHQ_XZ_HIS WHERE rq = "|| I_RQ ||") A,
                                 (SELECT JG.TZZH_ID,
                                         JG.KHH,
                                         JG.GDH,
                                         JG.JYS,
                                         JG.ZQDM,
                                         JG.JYLB,
                                         JG.ZQLB,
                                         JG.CJBH,
                                         JG.BZ,
                                         JG.CJRQ,
                                         JG.CJSL,
                                         JG.CJJE,
                                         JG.YSSL,
                                         JG.YSJE,
                                         JG.S1,
                                         JG.S2,
                                         JG.S3,
                                         JG.S4,
                                         JG.S5,
                                         JG.S6,
                                         CASE WHEN PG.ZQDM IS NOT NULL THEN 1 ELSE 0 END IS_PG,
                                         CASE WHEN PG.PGJG IS NOT NULL THEN PG.PGJG ELSE 0 END PGJG,
                                         CASE WHEN KG.ZQDM IS NOT NULL THEN 1 ELSE 0 END IS_KG,
                                         CASE WHEN D.JJDM IS NOT NULL AND JG.JYS != '2' THEN 1 ELSE 0 END IS_ETFHB
                                    FROM "|| F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH) ||" JG
                                    LEFT JOIN CUST.T_HBJJXX D ON (JG.JYS = D.JYS AND JG.ZQDM = D.JJDM)
                                    LEFT JOIN (select distinct cqcxrq,jys,zqdm from INFO.TGP_GSFHKG WHERE cqcxrq=" || I_RQ || " and jys='2') kg ON (kg.jys=jg.jys and kg.zqdm=jg.zqdm)
                                    LEFT JOIN (select distinct pgssrq,jys,zqdm,pgjg from INFO.TGP_GPPG WHERE pgssrq=" ||l_nextDay|| ") pg ON (pg.jys=jg.jys and pg.zqdm=jg.zqdm)
                                   WHERE JG.JYLB IN ('7',
                                                     '9',
                                                     '10',
                                                     '12',
                                                     '13',
                                                     '15',
                                                     '18',
                                                     '19',
                                                     '31',
                                                     '37',
                                                     '38',
                                                     '47',
                                                     '48',
                                                     '65',
                                                     '66',
                                                     '21',
                                                     '20')
                                     AND JG.ZQDM NOT IN ('888880', '900000')
                                     AND JG.CJBH NOT IN
                                      ('折算变更', '比例变更')
                                     AND JG.CJBH NOT IN (
                                                         '配股上市',
                                                         '送股上市',
                                                         '送股托管',
                                                         '新股托管')) B
                           WHERE A.JYS = B.JYS
                             AND A.ZQDM = B.ZQDM
                           GROUP BY B.TZZH_ID, B.KHH, B.GDH, B.JYS, B.ZQDM, B.BZ) A";


        l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCbjsDR', I_KHH) || l_columns || l_sqlBuf;
        EXECUTE IMMEDIATE l_sql;
    END;

    ---------------------------------------------5、基金赎回--------------------------------------------
    BEGIN
        --19 份额变动
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL" ||
                "   FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH) ||
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='19' AND CJBH='份额变动' " ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";
        l_tableName := F_IDS_GET_TABLENAME('xy_sparkJjshFebd', I_KHH);

        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

        --43 赎回确认
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL" ||
                 "   FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsXYQS', I_KHH) ||
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='43' AND CJBH='赎回确认'" ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";

        l_tableName := F_IDS_GET_TABLENAME('xy_sparkJjshqr', I_KHH);

        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

        --待交收
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL," ||
                "        SUM(YSJE_2) AS YSJE" ||
                "   FROM DSC_BAS.T_XY_DJSQSZL_HIS " ||
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='43' AND RQ = CJRQ AND RQ = " || I_RQ ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";

        l_tableName := F_IDS_GET_TABLENAME('xy_sparkJjshDjs', I_KHH);

        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

        --汇总
        l_sqlBuf := " SELECT  0 as TZZH_ID," ||
               --     I_RQ || " AS RQ," ||
                "         A.KHH," ||
                "         A.JYS," ||
                "         A.GDH," ||
                "         A.ZQDM," ||
                "         A.BZ," ||
                "         0-A.CJSL AS DRYSSL," ||
                "         0 AS DRMRJE," ||
                "         0 AS DRMRSL," ||
                "         D.YSJE AS DRMCJE," ||
                "         A.CJSL AS DRMCSL," ||
                "         0 AS DRJYFY," ||
                "         0 AS DRSHDQSL," ||
                "         0 AS DRSHDQJE," ||
                "         0 AS DRSHSYSL," ||
                "         0 AS SRSHSYSL," ||
                "         0 AS DRHBCFJE_ZR," ||
                "         0 AS DRHBCFSL_ZR," ||
                "         0 AS DRHBCFJE_ZC," ||
                "         0 AS DRHBCFSL_ZC," ||
                "         0 AS DRSHSL_ETFHB," ||
                "         0 AS DRSGSL_ETFHB," ||
                "         0 AS DRSHJE_SJ_ETFHB," ||
                "         0 AS DRSGJE_SJ_ETFHB," ||
                "         0 AS DRPGJE," ||
                "         0 AS DRPGSL," ||
                "         0 AS drshje_ss_etfhb," ||
                "         0 AS DRSGJE_SF_ETFHB" ||
                "      FROM " || F_IDS_GET_TABLENAME('xy_sparkJjshFebd', I_KHH) || " A " ||
                " LEFT JOIN "|| F_IDS_GET_TABLENAME('xy_sparkJjshDjs', I_KHH) || " D ON (A.KHH=D.KHH AND A.GDH=D.GDH AND A.BZ=D.BZ AND A.JYS=D.JYS AND A.ZQDM=D.ZQDM AND A.CJSL=D.CJSL)" ||
                "     WHERE NOT EXISTS (SELECT 1 FROM "|| F_IDS_GET_TABLENAME('xy_sparkJjshqr', I_KHH) || " B WHERE A.KHH=B.KHH AND A.GDH=B.GDH AND A.BZ=B.BZ AND A.JYS=B.JYS AND A.ZQDM=B.ZQDM AND A.CJSL=B.CJSL)";

        --写入当日成本计算表
        l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCbjsDR', I_KHH) || l_columns || l_sqlBuf;
        EXECUTE IMMEDIATE l_sql;
    END;

    BEGIN
        l_tableName:=F_IDS_GET_TABLENAME('xy_sparkHBJJSGSHsgRe', I_KHH);
         -- 获取临时表字段，并拼接(column1, column2, column3....)
        l_tableArr := get_columns(l_tableName);
        l_columns := '';
        FOR indx IN l_tableArr.first() .. l_tableArr.last() LOOP
          IF(l_tableArr(indx)<>'rq') THEN
              IF indx = l_tableArr.last() THEN
                l_columns := l_columns || l_tableArr(indx) ;
              ELSE
                l_columns := l_columns || l_tableArr(indx) || ',';
              END IF;
           END IF;
        END LOOP;
       l_columns_k:=' ( '||l_columns||' ) ';
    END;

    BEGIN
        l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCbjsDR', I_KHH)|| l_columns_k ||" SELECT "||l_columns||" FROM "||l_tableName ;
        EXECUTE IMMEDIATE l_sql;
    END;



  --8 生成当日成本盈亏
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkCbjsDRResult', I_KHH);
    l_sqlBuf := "SELECT
                  A.TZZH_ID,
                  A.KHH,
                  A.GDH,
                  A.JYS,
                  A.ZQDM,
                  A.ZQLB,
                  A.BZ,
                  NVL(NVL((CASE WHEN (A.DRMRSL>0 AND A.DRMRSL=A.LJMRSL AND A.ZQSL>0) OR
                  (A.DRMRSL>0 AND A.DRMRSL=A.LJMRSL AND A.DRMRSL=A.DRMCSL AND A.ZQSL = 0) THEN
                  " || I_RQ || "
                       ELSE NULL
                  END),A.KCRQ), " || I_RQ || " ) AS KCRQ,
                  A.ZQSL,
                  A.ZQSZ,
                  A.DRYSSL,
                  A.DRMRJE,
                  A.DRMRSL,
                  CAST(NULL AS DECIMAL(16,2)) AS DRPGJE,
                  A.DRPGSL,
                  A.DRMCJE,
                  A.DRMCSL,
                  A.DRJYFY,
                  CAST(NULL AS DECIMAL(16,2)) AS SRCCJJ,
                  A.LJMRJE,
                  A.LJMRSL,
                  CAST(NULL AS DECIMAL(16,2)) AS LJPGJE,
                  A.LJPGSL,
                  A.LJMCJE,
                  A.LJMCSL,
                  A.LJJYFY,
                  A.SRZQSL,
                  A.SRZQSZ,
                  A.SRCBJ,
                  A.SRCCCB,
                  A.SRLJYK,
                  CAST(ROUND(CASE WHEN A.DRMRSL> A.DRMCSL THEN A.SRCCCB+A.DRMRJE-A.DRMCJE
                             WHEN A.DRMRSL< A.DRMCSL THEN A.SRCCCB-A.SRCBJ*(A.DRMCSL-A.DRMRSL)
                             ELSE A.SRCCCB
                         END,2) AS DECIMAL(16,2)) AS CCCB,
                  CAST(ROUND(CASE WHEN A.DRMRSL> A.DRMCSL THEN A.SRLJYK
                             WHEN A.DRMRSL< A.DRMCSL THEN A.SRLJYK+(A.DRMCJE-A.DRMRJE)-A.SRCBJ*(A.DRMCSL-A.DRMRSL)
                             ELSE A.SRLJYK+(A.DRMCJE-A.DRMRJE)
                         END,2) AS DECIMAL(16,2)) AS LJYK,
                  CAST(NULL AS DECIMAL(16,2)) AS CCCB_TZ,
                  CAST(NULL AS DECIMAL(16,2)) AS LJYK_TZ,
                  A.DRHBCFJE_ZR,
                  A.DRHBCFSL_ZR,
                  A.DRHBCFJE_ZC,
                  A.DRHBCFSL_ZC,
                  CAST(NULL AS DECIMAL(16,2)) AS DRHBCFJE_ZR_TZ,
                  CAST(NULL AS DECIMAL(16,2)) AS DRHBCFJE_ZC_TZ,
                  CAST(NULL AS DECIMAL(16,2)) AS YK_ZR,
                  CAST(NULL AS DECIMAL(16,2)) AS YK_ZC,
                  A.DRSHDQSL,
                  A.DRSHDQJE,
                  A.DRSHSYSL,
                  A.SRSHSYSL,
                  CASE WHEN A.ZQSL=0 AND (A.DRMCSL>0 OR (A.DRSHDQSL>0 AND A.SRZQSL>0 AND A.DRSHDQSL<>A.SRSHSYSL)) AND A.DRSHSL_ETFHB>0 THEN
                  "|| l_nextDay || "
                       WHEN A.ZQSL=0 AND (A.DRMCSL>0 OR (A.DRSHDQSL>0 AND A.SRZQSL>0 AND A.DRSHDQSL<>A.SRSHSYSL)) THEN
                   " || I_RQ || "
                       ELSE 0
                  END AS QCRQ,
                  A.DRSHSL_ETFHB,
                  A.DRSGSL_ETFHB,
                  A.DRSHJE_SJ_ETFHB,
                  A.DRSGJE_SJ_ETFHB,
                  A.DRSHJE_SS_ETFHB,
                  A.DRSGJE_SF_ETFHB,
                  A.LJSHJE_SJ_ETFHB,
                  A.LJSGJE_SJ_ETFHB,
                  A.LJSHJE_SS_ETFHB,
                  A.LJSGJE_SF_ETFHB,
                "|| I_RQ || "  as RQ
                   FROM
                  (SELECT A.TZZH_ID,
                           A.KHH,
                           A.JYS,
                           A.GDH,
                           A.ZQDM,
                           Z.ZQLB AS ZQLB,
                           A.BZ,
                           A.KCRQ,
                           NVL(B.ZQSL,0) AS ZQSL,
                           NVL(B.ZXSZ,0) AS ZQSZ,
                           CAST(NVL(DRYSSL,0) AS DECIMAL(22,2)) AS DRYSSL,
                           CAST(NVL(DRMRJE,0) AS DECIMAL(16,2)) AS DRMRJE,
                           CAST(NVL(DRMRSL,0) AS DECIMAL(22,2)) AS DRMRSL,
                           CAST(NVL(DRMCJE,0) AS DECIMAL(16,2)) AS DRMCJE,
                           CAST(NVL(DRMCSL,0) AS DECIMAL(22,2)) AS DRMCSL,
                           CAST(NVL(DRPGSL,0) AS DECIMAL(22,2)) AS DRPGSL,
                           CAST(NVL(DRJYFY,0) AS DECIMAL(16,2)) AS DRJYFY,
                           CAST(NVL(DRHBCFJE_ZR,0) AS DECIMAL(16,2)) AS DRHBCFJE_ZR,
                           CAST(NVL(DRHBCFSL_ZR,0) AS DECIMAL(16,2)) AS DRHBCFSL_ZR,
                           CAST(NVL(DRHBCFJE_ZC,0) AS DECIMAL(16,2)) AS DRHBCFJE_ZC,
                           CAST(NVL(DRHBCFSL_ZC,0) AS DECIMAL(16,2)) AS DRHBCFSL_ZC,
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMRJE,0),NVL(LJMRJE,0)) AS DECIMAL(16,2)) AS LJMRJE,-- 考虑当日买入卖出清仓情况处理
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMRSL,0),NVL(LJMRSL,0)) AS DECIMAL(22,2)) AS LJMRSL,-- 考虑当日买入卖出清仓情况处理
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMCJE,0),NVL(LJMCJE,0)) AS DECIMAL(16,2)) AS LJMCJE,-- 考虑当日买入卖出清仓情况处理
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMCSL,0),NVL(LJMCSL,0)) AS DECIMAL(22,2)) AS LJMCSL,-- 考虑当日买入卖出清仓情况处理
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRPGSL,0),NVL(LJPGSL,0)) AS DECIMAL(22,2)) AS LJPGSL,-- 考虑当日买入卖出清仓情况处理
                           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRJYFY,0),NVL(LJJYFY,0)) AS DECIMAL(16,2)) AS LJJYFY,-- 考虑当日买入卖出清仓情况处理
                           NVL(C.CCCB,0) SRCCCB,
                           NVL(C.LJYK,0) SRLJYK,
                           NVL(C.ZQSL,0) SRZQSL,
                           NVL(C.ZXSZ,0) SRZQSZ,
                           CAST(ROUND(IF(NVL(C.ZQSL,0) > 0, NVL(C.CCCB,0)/NVL(C.ZQSL,0), 0),4) AS DECIMAL(9,4)) AS SRCBJ,
                           CAST(NVL(DRSHDQSL,0) AS DECIMAL(16,2)) AS DRSHDQSL,
                           CAST(NVL(DRSHDQJE,0) AS DECIMAL(16,2)) AS DRSHDQJE,
                           CAST(NVL(DRSHSYSL,0) AS DECIMAL(16,2)) AS DRSHSYSL,
                           CAST(NVL(SRSHSYSL,0) AS DECIMAL(16,2)) AS SRSHSYSL,
                           CAST(NVL(DRSHSL_ETFHB,0) AS DECIMAL(16,2)) AS DRSHSL_ETFHB,
                           CAST(NVL(DRSGSL_ETFHB,0) AS DECIMAL(16,2)) AS DRSGSL_ETFHB,
                           CAST(NVL(DRSHJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS DRSHJE_SJ_ETFHB,
                           CAST(NVL(DRSGJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS DRSGJE_SJ_ETFHB,
                           CAST(NVL(LJSHJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS LJSHJE_SJ_ETFHB,
                           CAST(NVL(LJSGJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS LJSGJE_SJ_ETFHB,
                           CAST(NVL(DRSHJE_SS_ETFHB,0) AS DECIMAL(16,2)) AS DRSHJE_SS_ETFHB,
                           CAST(NVL(DRSGJE_SF_ETFHB,0) AS DECIMAL(16,2)) AS DRSGJE_SF_ETFHB,
                           CAST(NVL(LJSHJE_SS_ETFHB,0) AS DECIMAL(16,2)) AS LJSHJE_SS_ETFHB,
                           CAST(NVL(LJSGJE_SF_ETFHB,0) AS DECIMAL(16,2)) AS LJSGJE_SF_ETFHB
                     FROM (SELECT D.TZZH_ID,
                           D.KHH,
                           D.JYS,
                           D.GDH,
                           D.ZQDM,
                           D.BZ,
                           NVL(SUM(KCRQ),0) KCRQ,
                           NVL(SUM(DRYSSL),0) DRYSSL,
                           NVL(SUM(DRMRJE),0) DRMRJE,
                           NVL(SUM(DRMRSL),0) DRMRSL,
                           NVL(SUM(DRMCJE),0) DRMCJE,
                           NVL(SUM(DRMCSL),0) DRMCSL,
                           NVL(SUM(DRPGSL),0) DRPGSL,
                           NVL(SUM(DRJYFY),0) DRJYFY,
                           NVL(SUM(DRHBCFJE_ZR),0) DRHBCFJE_ZR,
                           NVL(SUM(DRHBCFSL_ZR),0) DRHBCFSL_ZR,
                           NVL(SUM(DRHBCFJE_ZC),0) DRHBCFJE_ZC,
                           NVL(SUM(DRHBCFSL_ZC),0) DRHBCFSL_ZC,
                           NVL(SUM(LJMRJE),0) LJMRJE,
                           NVL(SUM(LJMRSL),0) LJMRSL,
                           NVL(SUM(LJMCJE),0) LJMCJE,
                           NVL(SUM(LJMCSL),0) LJMCSL,
                           NVL(SUM(LJPGSL),0) LJPGSL,
                           NVL(SUM(LJJYFY),0) LJJYFY,
                           NVL(SUM(DRSHDQSL),0) DRSHDQSL,
                           NVL(SUM(DRSHDQJE),0) DRSHDQJE,
                           NVL(SUM(DRSHSYSL),0) DRSHSYSL,
                           NVL(SUM(SRSHSYSL),0) SRSHSYSL,
                           NVL(SUM(DRSHSL_ETFHB),0) AS DRSHSL_ETFHB,
                           NVL(SUM(DRSGSL_ETFHB),0) AS DRSGSL_ETFHB,
                           NVL(SUM(DRSHJE_SJ_ETFHB),0) AS DRSHJE_SJ_ETFHB,
                           NVL(SUM(DRSGJE_SJ_ETFHB),0) AS DRSGJE_SJ_ETFHB,
                           NVL(SUM(LJSHJE_SJ_ETFHB),0) AS LJSHJE_SJ_ETFHB,
                           NVL(SUM(LJSGJE_SJ_ETFHB),0) AS LJSGJE_SJ_ETFHB,
                           NVL(SUM(DRSHJE_SS_ETFHB),0) AS DRSHJE_SS_ETFHB,
                           NVL(SUM(DRSGJE_SF_ETFHB),0) AS DRSGJE_SF_ETFHB,
                           NVL(SUM(LJSHJE_SS_ETFHB),0) AS LJSHJE_SS_ETFHB,
                           NVL(SUM(LJSGJE_SF_ETFHB),0) AS LJSGJE_SF_ETFHB
                      FROM (SELECT TZZH_ID,
                                   KHH,
                                   JYS,
                                   GDH,
                                   ZQDM,
                                   BZ,
                                   0 AS KCRQ,
                                   SUM(DRYSSL) AS DRYSSL,
                                   SUM(DRMRJE) AS DRMRJE,
                                   SUM(DRMRSL) AS DRMRSL,
                                   SUM(DRMCJE) AS DRMCJE,
                                   SUM(DRMCSL) AS DRMCSL,
                                   SUM(DRPGSL) AS DRPGSL,
                                   SUM(DRJYFY) AS DRJYFY,
                                   SUM(DRHBCFJE_ZR) AS DRHBCFJE_ZR,
                                   SUM(DRHBCFSL_ZR) AS DRHBCFSL_ZR,
                                   SUM(DRHBCFJE_ZC) AS DRHBCFJE_ZC,
                                   SUM(DRHBCFSL_ZC) AS DRHBCFSL_ZC,
                                   SUM(DRMRJE) AS LJMRJE,
                                   SUM(DRMRSL) AS LJMRSL,
                                   SUM(DRMCJE) AS LJMCJE,
                                   SUM(DRMCSL) AS LJMCSL,
                                   SUM(DRPGSL) AS LJPGSL,
                                   SUM(DRJYFY) AS LJJYFY,
                                   SUM(DRSHDQSL) AS DRSHDQSL,
                                   SUM(DRSHDQJE) AS DRSHDQJE,
                                   SUM(DRSHSYSL) AS DRSHSYSL,
                                   SUM(SRSHSYSL) AS SRSHSYSL,
                                   SUM(DRSHSL_ETFHB) AS DRSHSL_ETFHB,
                                   SUM(DRSGSL_ETFHB) AS DRSGSL_ETFHB,
                                   SUM(DRSHJE_SJ_ETFHB) AS DRSHJE_SJ_ETFHB,
                                   SUM(DRSGJE_SJ_ETFHB) AS DRSGJE_SJ_ETFHB,
                                   SUM(DRSHJE_SJ_ETFHB) AS LJSHJE_SJ_ETFHB,
                                   SUM(DRSGJE_SJ_ETFHB) AS LJSGJE_SJ_ETFHB,
                                   SUM(DRSHJE_SS_ETFHB) AS DRSHJE_SS_ETFHB,
                                   SUM(DRSGJE_SF_ETFHB) AS DRSGJE_SF_ETFHB,
                                   SUM(DRSHJE_SS_ETFHB) AS LJSHJE_SS_ETFHB,
                                   SUM(DRSGJE_SF_ETFHB) AS LJSGJE_SF_ETFHB
                              FROM  " || F_IDS_GET_TABLENAME('xy_sparkCbjsDR', I_KHH) || "
                             GROUP BY TZZH_ID, KHH, JYS, GDH, ZQDM,BZ
                            UNION ALL
                            SELECT TZZH_ID,
                                   KHH,
                                   JYS,
                                   GDH,
                                   ZQDM,
                                   BZ,
                                   KCRQ,
                                   0 AS DRYSSL,
                                   0 AS DRMRJE,
                                   0 AS DRMRSL,
                                   0 AS DRMCJE,
                                   0 AS DRMCSL,
                                   0 AS DRPGSL,
                                   0 AS DRJYFY,
                                   0 AS DRHBCFJE_ZR,
                                   0 AS DRHBCFSL_ZR,
                                   0 AS DRHBCFJE_ZC,
                                   0 AS DRHBCFSL_ZC,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJMRJE,0)) LJMRJE,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJMRSL,0)) LJMRSL,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJMCJE,0)) LJMCJE,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJMCSL,0)) LJMCSL,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJPGSL,0)) LJPGSL,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJJYFY,0)) LJJYFY,
                                   0 AS DRSHDQSL,
                                   0 AS DRSHDQJE,
                                   0 AS DRSHSYSL,
                                   0 AS SRSHSYSL,
                                   0 AS DRSHSL_ETFHB,
                                   0 AS DRSGSL_ETFHB,
                                   0 AS DRSHJE_SJ_ETFHB,
                                   0 AS DRSGJE_SJ_ETFHB,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJSHJE_SJ_ETFHB,0)) LJSHJE_SJ_ETFHB,
                                   IF(NVL(ZQSL,0)=0,0,NVL(LJSGJE_SJ_ETFHB,0)) LJSGJE_SJ_ETFHB,
                                   0 AS DRSHJE_SS_ETFHB,
                                   0 AS DRSGJE_SF_ETFHB,
                                   LJSHJE_SS_ETFHB,
                                   LJSGJE_SF_ETFHB
                              FROM " || F_IDS_GET_TABLENAME('xy_sparkCbjsPre1', I_KHH) || " where zqsl!=0) D
                     GROUP BY D.TZZH_ID, D.BZ, D.KHH, D.JYS, D.GDH, D.ZQDM) A LEFT JOIN
                                  " || F_IDS_GET_TABLENAME('xy_sparkZqyeDR', I_KHH) || " B
                      on (A.JYS=B.JYS AND A.ZQDM=B.ZQDM AND A.GDH=B.GDH AND A.TZZH_ID=B.TZZH_ID AND A.KHH = B.KHH)
                                  LEFT JOIN
                                   " || F_IDS_GET_TABLENAME('xy_sparkZqyeSR', I_KHH) || " c
                      on (A.JYS=c.JYS AND A.ZQDM=c.ZQDM AND A.GDH=c.GDH AND A.TZZH_ID=c.TZZH_ID AND A.KHH = C.KHH)
                                  LEFT JOIN CUST.T_ZQDM Z
                      ON (A.JYS=Z.JYS AND A.ZQDM=Z.ZQDM))A";

   F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_XY_ZQYE_CBJS', I_RQ, I_KHH);
  END;

  --9 处理持仓数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZqyeResult', I_KHH);
    l_sqlBuf := "SELECT Z.TZZH_ID,
                         Z.KHH,
                         Z.JYS,
                         Z.GDH,
                         Z.ZQDM,
                         Z.KCRQ,
                         Z.ZQLB,
                         Z.BZ,
                         Z.ZQSL,
                         Z.FLTSL,
                         Z.ZXSZ,
                         Z.CCCB,
                         CAST((CASE
                                WHEN Z.ZQSL <> 0 THEN
                                    ROUND(CAST(IF(Z.ZQSL = 0, 0, Z.CCCB / Z.ZQSL) AS decimal(16,10)), 4)
                                ELSE
                                 Z.CBJ
                              END) AS DECIMAL(9, 4)) AS CBJ,
                         Z.LJYK,
                         CAST((CASE
                                WHEN Z.ZQSL <> 0 THEN
                                 Z.CCCB - Z.LJYK
                                ELSE
                                 Z.TBCCCB
                              END) AS DECIMAL(16, 2)) AS TBCCCB,
                         CAST((CASE
                                WHEN Z.ZQSL <> 0 THEN
                                    ROUND(cast(IF(Z.ZQSL = 0, 0, (Z.CCCB - Z.LJYK) / Z.ZQSL) AS decimal(16, 10)), 4)
                                ELSE
                                 Z.TBCBJ
                              END) AS DECIMAL(16, 4)) AS TBCBJ,
                         Z.DRYK,
                         Z.ZQSL_ORIG,
                "|| I_RQ || "  as RQ
                    FROM (SELECT Z.TZZH_ID,
                                 Z.RQ,
                                 Z.KHH,
                                 Z.JYS,
                                 Z.GDH,
                                 Z.ZQDM,
                                 CASE
                                   WHEN C.KHH IS NOT NULL THEN
                                    C.KCRQ
                                   ELSE
                                    Z.KCRQ
                                 END AS KCRQ,
                                 Z.ZQLB,
                                 Z.BZ,
                                 Z.ZQSL,
                                 Z.FLTSL,
                                 Z.ZXSZ,
                                 CASE
                                   WHEN C.KHH IS NOT NULL THEN
                                    NVL(C.CCCB, 0)
                                   ELSE
                                    Z.CCCB
                                 END AS CCCB,
                                 Z.CBJ,
                                 CASE
                                   WHEN C.KHH IS NOT NULL THEN
                                    NVL(C.LJYK, 0)
                                   ELSE
                                    Z.LJYK
                                 END AS LJYK,
                                 Z.TBCCCB,
                                 Z.TBCBJ,
                                 CAST(CASE
                                        WHEN C.KHH IS NOT NULL THEN
                                         NVL(C.ZQSZ - C.SRZQSZ - (C.DRMRJE - C.DRMCJE), 0)
                                        ELSE
                                         Z.DRYK
                                      END AS DECIMAL(16, 2)) AS DRYK,
                                 Z.ZQSL_ORIG
                            FROM " || F_IDS_GET_TABLENAME('xy_sparkZqyeDR', I_KHH) || " Z
                            LEFT JOIN " || F_IDS_GET_TABLENAME('xy_sparkCbjsDRResult', I_KHH) || " C
                              ON (Z.JYS = C.JYS AND Z.ZQDM = C.ZQDM AND
                                 Z.GDH = C.GDH AND Z.TZZH_ID = C.TZZH_ID AND Z.KHH = C.KHH)
                           ) Z
                    LEFT JOIN CUST.T_ZQDM D
                      ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)
                  WHERE Z.ZQSL <> 0";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_XY_ZQYE_HIS', I_RQ, I_KHH);
  END;

  --10 获取当天清仓数据,并写到目标表
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkTzsy', I_KHH);
    l_sqlBuf := "SELECT
                       T.KCRQ,
                       T.TZZH_ID,
                       T.KHH,
                       T.GDH,
                       T.JYS,
                       T.ZQDM,
                       IF(D.ZQDM IS NULL, T.ZQLB, D.ZQLB) AS ZQLB,
                       T.BZ,
                       T.MRJE,
                       T.MCJE,
                       T.JYFY,
                       T.LJYK,
                       T.DRYK,
                "|| I_RQ ||"  as QCRQ
                  FROM (SELECT
                               D.KCRQ,
                               D.TZZH_ID,
                               D.KHH,
                               D.JYS,
                               D.GDH,
                               D.ZQDM,
                               D.ZQLB,
                               D.BZ,
                               D.LJMRJE AS MRJE,
                               D.LJMCJE AS MCJE,
                               D.LJJYFY AS JYFY,
                               CASE
                                 WHEN IS_ETFHBSH = 1 THEN
                                  D.LJMCJE - D.LJMRJE
                                 ELSE
                                  NVL(D.LJYK_TZ, D.LJYK)
                               END AS LJYK,
                               CASE
                                 WHEN  " || I_RQ || "  = KCRQ AND IS_ETFHBSH = 1 THEN
                                  D.LJMCJE - D.LJMRJE
                                 ELSE
                                  D.ZQSZ - D.SRZQSZ - (NVL(D.DRMRJE_TZ, D.DRMRJE) -
                                  NVL(D.DRMCJE_TZ, D.DRMCJE))
                               END AS DRYK
                          FROM (SELECT
                                       T.KHH,
                                       T.TZZH_ID,
                                       T.GDH,
                                       T.BZ,
                                       T.JYS,
                                       T.ZQDM,
                                       T.ZQLB,
                                       T.KCRQ,
                                       T.ZQSL,
                                       T.DRMCSL,
                                       T.LJMRJE - NVL(T.LJSGJE_SJ_ETFHB, 0) +
                                       NVL(T.LJSGJE_SF_ETFHB, 0) AS LJMRJE,
                                       T.LJMCJE - NVL(T.LJSHJE_SJ_ETFHB, 0) +
                                       NVL(T.LJSHJE_SS_ETFHB, 0) AS LJMCJE,
                                       T.LJJYFY,
                                       T.CCCB,
                                       T.LJYK,
                                       T.CCCB_TZ,
                                       T.LJYK_TZ,
                                       T.ZQSZ,
                                       T.SRZQSL,
                                       T.SRZQSZ,
                                       T.DRMRJE,
                                       T.DRMCJE,
                                       T.DRSHDQSL,
                                       T.DRSHDQJE,
                                       T.DRSHSYSL,
                                       T.SRSHSYSL,
                                       (T.DRMRJE - NVL(T.DRHBCFJE_ZR, 0) +
                                       NVL(T.DRHBCFJE_ZR_TZ, 0) - NVL(T.YK_ZR, 0)) AS DRMRJE_TZ,
                                       CASE
                                         WHEN (DRSHDQSL > 0 AND SRZQSL > 0 AND
                                              DRSHDQSL <> SRSHSYSL) THEN

                                          (T.DRMCJE - NVL(T.DRHBCFJE_ZC, 0) +
                                          NVL(T.DRHBCFJE_ZC_TZ, 0) - NVL(T.YK_ZC, 0) +
                                          NVL(DRSHDQJE, 0))
                                         ELSE

                                          (T.DRMCJE - NVL(T.DRHBCFJE_ZC, 0) +
                                          NVL(T.DRHBCFJE_ZC_TZ, 0) - NVL(T.YK_ZC, 0))
                                       END AS DRMCJE_TZ,
                                       T.QCRQ,
                                       CASE
                                         WHEN NVL(T.LJSHJE_SS_ETFHB, 0) <> 0 OR
                                              NVL(T.LJSGJE_SF_ETFHB, 0) <> 0 THEN
                                          1
                                         ELSE
                                          0
                                       END AS IS_ETFHBSH
                                  FROM " || F_IDS_GET_TABLENAME('xy_sparkCbjsDRResult', I_KHH) || " T
                                 WHERE T.QCRQ =  "|| I_RQ ||"  ) D
                        UNION ALL
                        SELECT CJRQ AS KCRQ,
                               0 AS TZZH_ID,
                               KHH,
                               JYS,
                               GDH,
                               ZQDM,
                               ZQLB,
                               BZ,
                               CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE_2) ELSE ABS(YSJE) END) AS DECIMAL(18,2)) AS MRJE,
                               CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE) ELSE CJJE + LXJE END) AS DECIMAL(18,2)) AS MCJE,
                               0 AS JYFY,
                               CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE)-ABS(YSJE_2) ELSE LXJE-(ABS(YSJE_2)-CJJE) END) AS DECIMAL(18,2)) AS LJYK,
                               CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE)-ABS(YSJE_2) ELSE LXJE-(ABS(YSJE_2)-CJJE) END) AS DECIMAL(18,2)) AS DRYK
                          FROM " || F_IDS_GET_TABLENAME('xy_sparkDjsT', I_KHH) || " T
                         WHERE ZQLB IN ('H0', 'H1', 'H3', 'H4')
                         GROUP BY SETTLE_DATE_2,

                                  CJRQ,
                                  KHH,
                                  GDH,
                                  JYS,
                                  ZQDM,
                                  ZQLB,
                                  BZ) T
                  LEFT JOIN CUST.T_ZQDM D
                    ON (T.JYS = D.JYS AND T.ZQDM = D.ZQDM)
                  LEFT JOIN " || F_IDS_GET_TABLENAME('xy_sparkCbjsDRResult', I_KHH) || " C
                    ON ((C.DRHBCFSL_ZC > 0 OR C.DRHBCFSL_ZR > 0) AND
                       C.KHH = T.KHH AND C.JYS = T.JYS AND C.ZQDM = T.ZQDM)
                 WHERE NOT
                        ((T.DRYK = 0 AND T.LJYK = 0 AND T.ZQLB IN ('L0')) AND C.KHH IS NOT NULL)";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_XY_TZSY', I_RQ, I_KHH);
  END;
END;

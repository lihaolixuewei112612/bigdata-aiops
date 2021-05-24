CREATE OR REPLACE PROCEDURE cust.p_ids_xy_cust_daily_stat(
--输入变量
I_RQ IN INT,
I_KHH IN STRING
)
IS
/******************************************************************
  *文件名称：CUST.P_IDS_XY_CUST_DAILY_STAT
  *项目名称：IDS计算
  *文件说明：融资融券-信用资产指标统计

  创建人：燕居庆
  功能说明：融资融券-信用资产指标统计

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/20           创建
  王睿驹        v1.0.1        2019/8/22        根据java代码修改
  燕居庆        v1.0.2        2019/9/25        对比java-ids 5051版本
                                                1.增加成本计算
                                                2.增加清仓统计
                                                3.增加无持仓红利
                                                4.增加 HLXZ,  zcsz_yk 字段
                                                5.转出市值-盈亏修正值
 燕居庆         v1.0.3        2019/11/15        增加其他收入、其他支出
 燕居庆         v1.0.4        2020/2/26         修改：融资负债、融券负债计算公式调整
*******************************************************************/
l_sqlBuf STRING;    --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING;
l_sqlWhereLastDay STRING;
l_lastDay INT;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
l_sql STRING;
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
l_zhlb STRING;
BEGIN

  -- 获取上日交易日
  BEGIN
    SELECT f_get_jyr_date(I_RQ, -1) INTO l_lastDay FROM system.dual;
  END;

  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay;
  ELSE
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
  END IF;


  BEGIN
    --交割清算数据

    l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsHis', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_XY_JGMXLS_HIS WHERE CJRQ = ' || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;


  BEGIN
    --0.7 上日资产统计
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZcSR', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_STAT_XY_ZC_R WHERE RQ = ' || l_sqlWhereLastDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);


    l_zhlb:=' and  zhlb = 2';

    -- 资金存取
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZjcq', I_KHH);
    l_sqlBuf := 'select * from dsc_stat.t_stat_zjcqk_r z WHERE RQ = ' || l_sqlWhereCurrentDay||l_zhlb;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    -- 市值转入转出
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZczrzc', I_KHH);
    l_sqlBuf := 'select * from dsc_stat.t_stat_zqzrzc_r z WHERE RQ = ' || l_sqlWhereCurrentDay||l_zhlb;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    --成本计算
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZqyeCbjs', I_KHH);
    l_sqlBuf := "select * from CUST.T_XY_ZQYE_CBJS WHERE RQ = " || l_sqlWhereCurrentDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    --清仓统计
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkTzsy', I_KHH);
    l_sqlBuf := "select * from cust.t_xy_tzsy where qcrq = " || l_sqlWhereCurrentDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

  --1 生成资金余额数据
  BEGIN
    --1.1 获取汇率参数
    SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
    SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
  END;

  BEGIN
    --1.2 获取资金余额数据
    --l_tableName := F_IDS_GET_TABLENAME('xy_sparkZjye', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       KHH,
                       cast(0 AS decimal(16,2)) AS ZZC_SR,
                       cast(0 AS decimal(16,2)) AS ZZC,
                       cast(0 AS decimal(16,2)) AS ZQSZ,
                       CAST(ROUND(SUM(CASE
                                          WHEN BZ = '2' THEN
                                           ZHYE * " || l_hlcsHKD || "
                                          WHEN BZ = '3' THEN
                                           ZHYE * " || l_hlcsUSD || "
                                          ELSE
                                           ZHYE
                                      END),
                                  2) AS DECIMAL(16, 2)) AS ZJYE,
                       cast(0 AS decimal(16,2)) AS ZTZC,
                       cast(0 AS decimal(16,2)) AS QTZC,
                       cast(0 AS decimal(16,2)) AS CRJE,
                       cast(0 AS decimal(16,2)) AS QCJE,
                       cast(0 AS decimal(16,2)) AS ZRSZ,
                       cast(0 AS decimal(16,2)) AS ZCSZ,
                       cast(0 AS decimal(16,2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       cast(0 AS decimal(16,2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2)) AS RZFZ,
                       cast(0 AS decimal(16,2)) AS RQFZ,
                       cast(0 AS decimal(16,2)) AS YJLX,
                       cast(0 AS decimal(16,2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                       cast(0 AS decimal(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       cast(0 AS decimal(16,2)) AS XZRQFZ,
                       cast(0 AS decimal(16,2)) AS XZRQYJLX,
                       cast(0 AS decimal(16,2)) AS XZRZYJLX,
                       cast(0 AS decimal(16,2)) AS XZFZ,
                       cast(0 AS decimal(16,2)) AS XZHKJE,
                       cast(0 AS decimal(16,2)) AS XZRQHKJE,
                       cast(0 AS decimal(16,2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       cast(0 AS decimal(16,2)) AS XZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRQGHLX,
                       cast(0 AS decimal(16,2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM CUST.T_XY_ZJYE_HIS T
                  WHERE RQ = " || l_sqlWhereCurrentDay || "
                 GROUP BY TZZH_ID, RQ, KHH";

    -- 创建资产统计临时表，将后续所有内容insert into至资产统计临时表
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH);

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

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

  --2 生成证券市值数据
  BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('xy_sparkZqsz', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       KHH,
                       cast(0 AS decimal(16,2)) AS ZZC_SR,
                       cast(0 AS decimal(16,2)) AS ZZC,
                       CAST(ROUND(SUM(CASE
                                          WHEN T.BZ = '2' THEN
                                           ZXSZ * " || l_hlcsHKD || "
                                          WHEN T.BZ = '3' THEN
                                           ZXSZ * " || l_hlcsUSD || "
                                          ELSE
                                           ZXSZ
                                      END),
                                  2) AS DECIMAL(16, 2)) AS ZQSZ,
                       cast(0 AS decimal(16,2)) AS ZJYE,
                       cast(0 AS decimal(16,2)) AS ZTZC,
                       cast(0 AS decimal(16,2)) AS QTZC,
                       cast(0 AS decimal(16,2)) AS CRJE,
                       cast(0 AS decimal(16,2)) AS QCJE,
                       cast(0 AS decimal(16,2)) AS ZRSZ,
                       cast(0 AS decimal(16,2)) AS ZCSZ,
                       cast(0 AS decimal(16,2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       CAST(ROUND(SUM(CASE
                                          WHEN T.BZ = '2' THEN
                                           NVL(CCCB, cast(0 AS decimal(16,2))) * " || l_hlcsHKD || "
                                          WHEN T.BZ = '3' THEN
                                           NVL(CCCB, cast(0 AS decimal(16,2))) * " || l_hlcsUSD || "
                                          ELSE
                                           NVL(CCCB, cast(0 AS decimal(16,2)))
                                      END),
                                  2) AS DECIMAL(16, 2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2)) AS RZFZ,
                       cast(0 AS decimal(16,2)) AS RQFZ,
                       cast(0 AS decimal(16,2)) AS YJLX,
                       cast(0 AS decimal(16,2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                       cast(0 AS decimal(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       cast(0 AS decimal(16,2)) AS XZRQFZ,
                       cast(0 AS decimal(16,2)) AS XZRQYJLX,
                       cast(0 AS decimal(16,2)) AS XZRZYJLX,
                       cast(0 AS decimal(16,2)) AS XZFZ,
                       cast(0 AS decimal(16,2)) AS XZHKJE,
                       cast(0 AS decimal(16,2)) AS XZRQHKJE,
                       cast(0 AS decimal(16,2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       cast(0 AS decimal(16,2)) AS XZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRQGHLX,
                       cast(0 AS decimal(16,2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM CUST.T_XY_ZQYE_HIS  T
                  WHERE T.RQ = " || l_sqlWhereCurrentDay || "
                 GROUP BY TZZH_ID, KHH";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --3 融资融券负债数据
  BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('xy_sparkFzxx', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       KHH,
                       cast(0 AS decimal(16,2)) AS ZZC_SR,
                       cast(0 AS decimal(16,2)) AS ZZC,
                       cast(0 AS decimal(16,2)) AS ZQSZ,
                       cast(0 AS decimal(16,2)) AS ZJYE,
                       cast(0 AS decimal(16,2)) AS ZTZC,
                       cast(0 AS decimal(16,2)) AS QTZC,
                       cast(0 AS decimal(16,2)) AS CRJE,
                       cast(0 AS decimal(16,2)) AS QCJE,
                       cast(0 AS decimal(16,2)) AS ZRSZ,
                       cast(0 AS decimal(16,2)) AS ZCSZ,
                       CAST(ROUND(SUM((RZFZ + RQFZ + YJLX + FXYJLX - GHLX) * CASE BZ
                                          WHEN '2' THEN
                                           " || l_hlcsHKD || "
                                          WHEN '3' THEN
                                           " || l_hlcsUSD || "
                                          ELSE
                                           1
                                      END),
                                  2) AS DECIMAL(16, 2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       cast(0 AS decimal(16,2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       CAST(ROUND(SUM((RZFZ) * CASE BZ
                                          WHEN '2' THEN
                                           " || l_hlcsHKD || "
                                          WHEN '3' THEN
                                           " || l_hlcsUSD || "
                                          ELSE
                                           1
                                      END),
                                  2) AS DECIMAL(16, 2)) AS RZFZ,
                       CAST(ROUND(SUM((RQFZ) * CASE BZ
                                          WHEN '2' THEN
                                           " || l_hlcsHKD || "
                                          WHEN '3' THEN
                                           " || l_hlcsUSD || "
                                          ELSE
                                           1
                                      END),
                                  2) AS DECIMAL(16, 2)) AS RQFZ,
                       CAST(ROUND(SUM((YJLX + FXYJLX) * CASE BZ
                                          WHEN '2' THEN
                                           " || l_hlcsHKD || "
                                          WHEN '3' THEN
                                           " || l_hlcsUSD || "
                                          ELSE
                                           1
                                      END),
                                  2) AS DECIMAL(16, 2)) AS YJLX,
                       CAST(ROUND(SUM((GHLX) * CASE BZ
                                          WHEN '2' THEN
                                           " || l_hlcsHKD || "
                                          WHEN '3' THEN
                                           " || l_hlcsUSD || "
                                          ELSE
                                           1
                                      END),
                                  2) AS DECIMAL(16, 2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                       cast(0 AS decimal(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       cast(0 AS decimal(16,2)) AS XZRQFZ,
                       cast(0 AS decimal(16,2)) AS XZRQYJLX,
                       cast(0 AS decimal(16,2)) AS XZRZYJLX,
                       cast(0 AS decimal(16,2)) AS XZFZ,
                       cast(0 AS decimal(16,2)) AS XZHKJE,
                       cast(0 AS decimal(16,2)) AS XZRQHKJE,
                       cast(0 AS decimal(16,2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       cast(0 AS decimal(16,2)) AS XZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRQGHLX,
                       CAST(ROUND(SUM(HKJE), 2) AS DECIMAL(16, 2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM (SELECT 0 AS TZZH_ID,
                               BZ,
                               CASE
                                   WHEN JYLB = '61' THEN
                                    CASE WHEN NVL(FZBJ, 0) = 0 THEN NVL(RZJE,0) ELSE NVL(FZBJ, 0) END - (NVL(HKJE, 0)-NVL(GHLX,0))
                                   ELSE
                                    cast(0 AS decimal(16,2))
                               END AS RZFZ,
                               /*融资负债(融资成交金额 + 融资费用)*/
                               CASE
                                   WHEN JYLB = '64' THEN
                                    CASE WHEN NVL(RQSL,0)=0 THEN 0 ELSE NVL(ZXSZ, 0) * (1.0-NVL(HQSL,0)/NVL(RQSL,0)) END
                                   ELSE
                                    cast(0 AS decimal(16,2))
                               END AS RQFZ,
                               /* 融券负债 = (融券成交数量 - 还券数量) * 昨收盘 + 融券费用 = 最新市值 + 融券费用*/
                               JYLB,
                               NVL(YJLX,0) + nvl(FDLX,0) AS YJLX,
                               NVL(GHLX, 0) AS GHLX ,
                               NVL(HKJE, 0) HKJE,
                               KHH,
                               NVL(FXYJLX, 0) FXYJLX
                          FROM CUST.T_XY_FZXX_HIS WHERE FZZT <> 3 AND RQ = " || l_sqlWhereCurrentDay || " ) T
                 GROUP BY TZZH_ID, KHH";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --4 存入资金、取出资金数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZjmxls', I_KHH);
    l_sqlBuf := "SELECT  0 as TZZH_ID,
                        KHH,
                        0 AS zzc_sr       ,
                        0 AS zzc         ,
                        0 AS zqsz         ,
                        0 AS zjye         ,
                        0 AS ztzc         ,
                        0 AS qtzc         ,
                       CAST(SUM(CASE WHEN sjkm = '101'
                                    THEN fsje * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS CRJE,
                       CAST(SUM(CASE WHEN sjkm = '102'
                                    THEN fsje * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS QCJE,
                       cast(0 AS decimal(16,2)) AS ZRSZ,
                       cast(0 AS decimal(16,2)) AS ZCSZ,
                       cast(0 AS decimal(16,2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       cast(0 AS decimal(16,2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2)) AS RZFZ,
                       cast(0 AS decimal(16,2)) AS RQFZ,
                       cast(0 AS decimal(16,2)) AS YJLX,
                       cast(0 AS decimal(16,2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                                  CAST(SUM(CASE WHEN sjkm IN ('105')
                                    THEN fsje * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN "  || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       CAST(SUM(CASE WHEN sjkm IN ('111')
                                    THEN fsje * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN "  || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS QTSR,
                                CAST(SUM(CASE WHEN sjkm IN ('112')
                                    THEN fsje * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN "  || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       cast(0 AS decimal(16,2)) AS XZRQFZ,
                       cast(0 AS decimal(16,2)) AS XZRQYJLX,
                       cast(0 AS decimal(16,2)) AS XZRZYJLX,
                       cast(0 AS decimal(16,2)) AS XZFZ,
                       cast(0 AS decimal(16,2)) AS XZHKJE,
                       cast(0 AS decimal(16,2)) AS XZRQHKJE,
                       cast(0 AS decimal(16,2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       cast(0 AS decimal(16,2)) AS XZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRQGHLX,
                       cast(0 AS decimal(16,2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM  "||F_IDS_GET_TABLENAME('xy_sparkZjcq', I_KHH)||" group by TZZH_ID,khh";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --5 生成市值转入、市值转出数据、买券还券金额 交割明细数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkJgmxlsQS', I_KHH);
    l_sqlBuf := "SELECT 0 as TZZH_ID,
                       KHH,
                       cast(0 AS decimal(16,2)) AS ZZC_SR,
                       cast(0 AS decimal(16,2)) AS ZZC,
                       cast(0 AS decimal(16,2)) AS ZQSZ,
                       cast(0 AS decimal(16,2)) AS ZJYE,
                       cast(0 AS decimal(16,2)) AS ZTZC,
                       cast(0 AS decimal(16,2)) AS QTZC,
                       cast(0 AS decimal(16,2)) AS CRJE,
                       cast(0 AS decimal(16,2)) AS QCJE,
                              CAST(SUM(CASE WHEN cqfx = '1'
                                    THEN zqsz * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS ZRSZ,
                       CAST(SUM(CASE WHEN cqfx = '2'
                                    THEN zqsz * CASE BZ
                                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE
                                                 1
                                               END
                                    ELSE 0
                                END) AS DECIMAL(16,2)) AS ZCSZ,
                       cast(0 AS decimal(16,2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       cast(0 AS decimal(16,2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2)) AS RZFZ,
                       cast(0 AS decimal(16,2)) AS RQFZ,
                       cast(0 AS decimal(16,2)) AS YJLX,
                       cast(0 AS decimal(16,2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                       cast(0 AS decimal(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       cast(0 AS decimal(16,2)) AS XZRQFZ,
                       cast(0 AS decimal(16,2)) AS XZRQYJLX,
                       cast(0 AS decimal(16,2)) AS XZRZYJLX,
                       cast(0 AS decimal(16,2)) AS XZFZ,
                       cast(0 AS decimal(16,2)) AS XZHKJE,
                       cast(0 AS decimal(16,2)) AS XZRQHKJE,
                       cast(0 AS decimal(16,2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       cast(0 AS decimal(16,2)) AS XZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRZGHLX,
                       cast(0 AS decimal(16,2)) AS XZRQGHLX,
                       cast(0 AS decimal(16,2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM "||F_IDS_GET_TABLENAME('xy_sparkZczrzc', I_KHH)||" T
                 GROUP BY  KHH";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --6 生成新增融券负债、新增融券预计利息、新增融资预计利息、新增负债
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkFzxxbdmx', I_KHH);
    l_sqlBuf := "SELECT 0 AS TZZH_ID,
                       D.KHH,
                       cast(0 AS decimal(16,2)) AS ZZC_SR,
                       cast(0 AS decimal(16,2)) AS ZZC,
                       cast(0 AS decimal(16,2)) AS ZQSZ,
                       cast(0 AS decimal(16,2)) AS ZJYE,
                       cast(0 AS decimal(16,2)) AS ZTZC,
                       cast(0 AS decimal(16,2)) AS QTZC,
                       cast(0 AS decimal(16,2)) AS CRJE,
                       cast(0 AS decimal(16,2)) AS QCJE,
                       cast(0 AS decimal(16,2)) AS ZRSZ,
                       cast(0 AS decimal(16,2)) AS ZCSZ,
                       cast(0 AS decimal(16,2)) AS ZFZ,
                       cast(0 AS decimal(16,2)) AS ZFZ_SR,
                       cast(0 AS decimal(16,2)) AS CCCB,
                       cast(0 AS decimal(16,2)) AS DRYK,
                       cast(0 AS decimal(16,2)) AS TZZH_FE,
                       cast(0 AS decimal(16,2)) AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE,
                       cast(0 AS decimal(16,2)) AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ,
                       cast(0 AS decimal(10,4)) AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ,
                       cast(0 AS decimal(22,4)) AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2)) AS RZFZ,
                       cast(0 AS decimal(16,2)) AS RQFZ,
                       cast(0 AS decimal(16,2)) AS YJLX,
                       cast(0 AS decimal(16,2)) AS GHLX,
                       cast(0 AS decimal(16,2)) AS YJLX_SR,
                       cast(0 AS decimal(16,2)) AS LXSR,
                       cast(0 AS decimal(16,2)) AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2)) AS JYL,
                       CAST(ROUND(SUM(D.XZRQFZ), 2) AS DECIMAL(16, 2)) AS XZRQFZ,
                       CAST(ROUND(SUM(D.XZRQYJLX), 2) AS DECIMAL(16, 2)) AS XZRQYJLX,
                       CAST(ROUND(SUM(D.XZRZYJLX), 2) AS DECIMAL(16, 2)) AS XZRZYJLX,
                       CAST(ROUND(SUM(D.XZRQFZ) + SUM(D.XZRQYJLX) + SUM(D.XZRZYJLX), 2) AS DECIMAL(16, 2)) AS XZFZ,
                       CAST(ROUND(SUM(D.XZHKJE), 2) AS DECIMAL(16, 2)) AS XZHKJE,
                       CAST(ROUND(SUM(D.XZRQHKJE), 2) AS DECIMAL(16, 2)) AS XZRQHKJE,
                       CAST(ROUND(SUM(D.XZRZHKJE), 2) AS DECIMAL(16, 2)) AS XZRZHKJE,
                       cast(0 AS decimal(16,2)) AS MQHQJE,
                       CAST(ROUND(SUM(D.XZGHLX), 2) AS DECIMAL(16, 2)) AS XZGHLX,
                       CAST(ROUND(SUM(D.XZRZGHLX), 2) AS DECIMAL(16, 2)) AS XZRZGHLX,
                       CAST(ROUND(SUM(D.XZRQGHLX), 2) AS DECIMAL(16, 2)) AS XZRQGHLX,
                       cast(0 AS decimal(16,2)) AS HKJE,
                       cast(0 AS decimal(16,2)) AS MQHKJE,
                       cast(0 as decimal(16,2)) AS HLXZ,
                       CAST(0 AS DECIMAL(16,2)) AS zcsz_yk
                  FROM CUST.T_XY_FZXXBDMX_HIS D
                  WHERE D.RQ = " || l_sqlWhereCurrentDay || "
                 GROUP BY D.KHH";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --7 上日资产统计
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZcSR', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       KHH,
                       ZZC       AS ZZC_SR,
                       cast(0 AS decimal(16,2))         AS ZZC,
                       cast(0 AS decimal(16,2))         AS ZQSZ,
                       cast(0 AS decimal(16,2))         AS ZJYE,
                       cast(0 AS decimal(16,2))         AS ZTZC,
                       cast(0 AS decimal(16,2))         AS QTZC,
                       cast(0 AS decimal(16,2))         AS CRJE,
                       cast(0 AS decimal(16,2))         AS QCJE,
                       cast(0 AS decimal(16,2))         AS ZRSZ,
                       cast(0 AS decimal(16,2))         AS ZCSZ,
                       cast(0 AS decimal(16,2))         AS ZFZ,
                       ZFZ       AS ZFZ_SR,
                       cast(0 AS decimal(16,2))         AS CCCB,
                       cast(0 AS decimal(16,2))         AS DRYK,
                       cast(0 AS decimal(16,2))         AS TZZH_FE,
                       TZZH_FE   AS TZZH_FE_SR,
                       cast(0 AS decimal(16,2))         AS TZZH_ZXFE,
                       TZZH_ZXFE AS TZZH_ZXFE_SR,
                       cast(0 AS decimal(10,4))         AS TZZH_ZXJZ,
                       TZZH_ZXJZ AS TZZH_ZXJZ_SR,
                       cast(0 AS decimal(22,4))         AS TZZH_LJJZ,
                       TZZH_LJJZ AS TZZH_LJJZ_SR,
                       cast(0 AS decimal(16,2))         AS RZFZ,
                       cast(0 AS decimal(16,2))         AS RQFZ,
                       cast(0 AS decimal(16,2))         AS YJLX,
                       cast(0 AS decimal(16,2))         AS GHLX,
                       YJLX      AS YJLX_SR,
                       cast(0 AS decimal(16,2))         AS LXSR,
                       cast(0 AS decimal(16,2))         AS LXZC_QT,
                       cast(0 AS decimal(16,2)) AS QTSR,
                       cast(0 AS decimal(16,2)) AS QTZC_FY,
                       cast(0 AS decimal(16,2))         AS JYL,
                       cast(0 AS decimal(16,2))         AS XZRQFZ,
                       cast(0 AS decimal(16,2))         AS XZRQYJLX,
                       cast(0 AS decimal(16,2))         AS XZRZYJLX,
                       cast(0 AS decimal(16,2))         AS XZFZ,
                       cast(0 AS decimal(16,2))         AS XZHKJE,
                       cast(0 AS decimal(16,2))         AS XZRQHKJE,
                       cast(0 AS decimal(16,2))         AS XZRZHKJE,
                       cast(0 AS decimal(16,2))         AS MQHQJE,
                       cast(0 AS decimal(16,2))         AS XZGHLX,
                       cast(0 AS decimal(16,2))         AS XZRZGHLX,
                       cast(0 AS decimal(16,2))         AS XZRQGHLX,
                       cast(0 AS decimal(16,2))         AS HKJE,
                       cast(0 AS decimal(16,2))         AS MQHKJE,
                       cast(0 as decimal(16,2))         AS HLXZ,
                       CAST(0 AS DECIMAL(16,2))         AS zcsz_yk
                  FROM  " || F_IDS_GET_TABLENAME('xy_sparkZcSR', I_KHH) || " T ";

    -- 数据插入至资产统计临时表xy_sparkCustDaily
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --12.无持仓红利的情况
  BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('xy_sparkhlxzJz', I_KHH);
    l_sqlBuf :=  "SELECT  0 TZZH_ID,  " ||
                "        KHH,  " ||
                "        0 AS zzc_sr       ," ||
                "        0 AS zzc         ," ||
                "        0 AS zqsz         ," ||
                "        0 AS ZJYE,  " ||
                "        0 AS ztzc         ," ||
                "        0 AS qtzc         ," ||
                "        0 AS crje         ," ||
                "        0 AS qcje         ," ||
                "        0 AS zrsz         ," ||
                "        0 AS zcsz         ," ||
                "        0 AS zfz         ," ||
                "        0 AS zfz_sr       ," ||
                "        0 AS cccb         ," ||
                "        0 AS dryk         ," ||
                "        0 AS tzzh_fe     ," ||
                "        0 AS tzzh_fe_sr   ," ||
                "        0 AS tzzh_zxfe   ," ||
                "        0 AS tzzh_zxfe_sr ," ||
                "        0 AS tzzh_zxjz   ," ||
                "        0 AS tzzh_zxjz_sr ," ||
                "        0 AS tzzh_ljjz   ," ||
                "        0 AS tzzh_ljjz_sr ," ||
                "        0 AS rzfz         ," ||
                "        0 AS rqfz         ," ||
                "        0 AS yjlx         ," ||
                "        0 AS ghlx         ," ||
                "        0 AS yjlx_sr     ," ||
                "        0 AS lxsr         ," ||
                "        0 AS lxzc_qt     ," ||
                "        0 AS qtsr     ," ||
                "        0 AS qtzc_fy     ," ||
                "        0 AS jyl         ," ||
                "        0 AS xzrqfz       ," ||
                "        0 AS xzrqyjlx     ," ||
                "        0 AS xzrzyjlx     ," ||
                "        0 AS xzfz         ," ||
                "        0 AS xzhkje       ," ||
                "        0 AS xzrqhkje     ," ||
                "        0 AS xzrzhkje     ," ||
                "        0 AS mqhqje       ," ||
                "        0 AS xzghlx       ," ||
                "        0 AS xzrzghlx     ," ||
                "        0 AS xzrqghlx     ," ||
                "        0 AS hkje         ," ||
                "        0 AS mqhkje        ," ||
                "        SUM(HLXZ) AS hlxz," ||
                "        0 AS zcsz_yk" ||
                " FROM " ||
                " (SELECT " ||
                "        KHH," ||
                "        CAST(CASE WHEN BZ = '2' THEN YSJE * " || l_hlcsHKD ||
                "                 WHEN BZ = '3' THEN YSJE * " || l_hlcsUSD ||
                "                 ELSE YSJE" ||
                "            END AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsHis', I_KHH) || " J " ||
                "  WHERE NOT EXISTS " ||
                "       (SELECT 1 FROM " ||
                "           (SELECT KHH,GDH,JYS,ZQDM FROM CUST.T_XY_ZQYE_HIS WHERE RQ = "|| l_sqlWhereCurrentDay ||
                "           UNION ALL" ||
                "           SELECT KHH,GDH,JYS,ZQDM FROM " || F_IDS_GET_TABLENAME('xy_sparkTzsy', I_KHH) ||") A" ||
                "         WHERE J.KHH=A.KHH AND J.GDH=A.GDH AND J.JYS=A.JYS AND J.ZQDM=A.ZQDM)" ||
                "    AND J.JYLB='6'" ||
                " UNION ALL " ||
                " SELECT" ||
                "        KHH," ||
                "        CAST(CASE WHEN BZ = '2' THEN YSJE * " || l_hlcsHKD ||
                "                 WHEN BZ = '3' THEN YSJE * " || l_hlcsUSD ||
                "                 ELSE YSJE" ||
                "            END AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM (SELECT j.KHH,j.GDH,j.JYS,j.ZQDM,j.bz,SUM(j.YSSL*NVL(h.ZXJ, 0)) YSJE,SUM(YSSL) YSSL FROM " || F_IDS_GET_TABLENAME('xy_sparkJgmxlsHis', I_KHH) || " j left join (select * from dsc_bas.t_zqhq_xz_his where rq = " || I_RQ || ")
 h on (j.jys=h.jys and j.zqdm=h.zqdm) WHERE j.JYLB='16' AND j.YSSL>0 GROUP BY j.KHH,j.GDH,j.JYS,j.ZQDM,j.bz) J" ||
                "  WHERE EXISTS(SELECT 1 FROM " || F_IDS_GET_TABLENAME('xy_sparkZqyeCbjs', I_KHH) || " A WHERE j.khh=a.khh and j.GDH=a.GDH and j.JYS=a.JYS and j.ZQDM=a.ZQDM and j.yssl=a.zqsl and j.yssl=a.dryssl)" ||
                " ) A" ||
                " GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

  --13、转出市值-盈亏修正值
  BEGIN
    l_sqlBuf := "SELECT 0 TZZH_ID,  " ||
                "        KHH,  " ||
                "        0 AS zzc_sr       ," ||
                "        0 AS zzc         ," ||
                "        0 AS zqsz         ," ||
                "        0 AS ZJYE,  " ||
                "        0 AS ztzc         ," ||
                "        0 AS qtzc         ," ||
                "        0 AS crje         ," ||
                "        0 AS qcje         ," ||
                "        0 AS zrsz         ," ||
                "        0 AS zcsz         ," ||
                "        0 AS zfz         ," ||
                "        0 AS zfz_sr       ," ||
                "        0 AS cccb         ," ||
                "        0 AS dryk         ," ||
                "        0 AS tzzh_fe     ," ||
                "        0 AS tzzh_fe_sr   ," ||
                "        0 AS tzzh_zxfe   ," ||
                "        0 AS tzzh_zxfe_sr ," ||
                "        0 AS tzzh_zxjz   ," ||
                "        0 AS tzzh_zxjz_sr ," ||
                "        0 AS tzzh_ljjz   ," ||
                "        0 AS tzzh_ljjz_sr ," ||
                "        0 AS rzfz         ," ||
                "        0 AS rqfz         ," ||
                "        0 AS yjlx         ," ||
                "        0 AS ghlx         ," ||
                "        0 AS yjlx_sr     ," ||
                "        0 AS lxsr         ," ||
                "        0 AS lxzc_qt     ," ||
                "        0 AS qtsr     ," ||
                "        0 AS qtzc_fy     ," ||
                "        0 AS jyl         ," ||
                "        0 AS xzrqfz       ," ||
                "        0 AS xzrqyjlx     ," ||
                "        0 AS xzrzyjlx     ," ||
                "        0 AS xzfz         ," ||
                "        0 AS xzhkje       ," ||
                "        0 AS xzrqhkje     ," ||
                "        0 AS xzrzhkje     ," ||
                "        0 AS mqhqje       ," ||
                "        0 AS xzghlx       ," ||
                "        0 AS xzrzghlx     ," ||
                "        0 AS xzrqghlx     ," ||
                "        0 AS hkje         ," ||
                "        0 AS mqhkje        ," ||
                "        0 AS hlxz," ||
                "       CAST(SUM(CASE" ||
                "             WHEN BZ = '2' THEN" ||
                "              ZCSZ_YK * " || l_hlcsHKD ||
                "             WHEN BZ = '3' THEN" ||
                "              ZCSZ_YK * " || l_hlcsUSD ||
                "             ELSE" ||
                "              ZCSZ_YK" ||
                "           END) AS DECIMAL(16,2)) AS ZCSZ_YK" ||
                " FROM " ||
                " (SELECT " ||
                "       Z.KHH," ||
                "       Z.BZ," ||
                "       Z.ZQSL/(Z.ZQSL+C.ZQSL)*C.LJYK AS ZCSZ_YK" ||
                " FROM (SELECT KHH,BZ,ZQDM,SUM(ZQSL) ZQSL FROM " || F_IDS_GET_TABLENAME('xy_sparkZczrzc', I_KHH) || " WHERE ZQSZ > 0 GROUP BY KHH,BZ,ZQDM) Z," ||
                "      (SELECT KHH,BZ,ZQDM,SUM(ZQSL) ZQSL,SUM(LJYK) LJYK FROM " || F_IDS_GET_TABLENAME('xy_sparkZqyeCbjs', I_KHH) || " GROUP BY KHH,BZ,ZQDM) C" ||
                " WHERE Z.KHH=C.KHH AND Z.ZQDM=C.ZQDM AND Z.BZ=C.BZ) a GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;

  END;
  --8 生成当日资产统计最终数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('xy_sparkZctjResult', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       KHH,
                       CAST(ROUND((CASE
                                      WHEN ZZC_SR = 0 AND ZZC > 0 THEN
                                       ZZC - DRYK
                                      ELSE
                                       ZZC_SR
                                  END),
                                  2) AS DECIMAL(16, 2)) AS ZZC_SR,
                       CAST(ROUND(ZZC, 2) AS DECIMAL(16, 2)) AS ZZC,
                       CAST(ROUND(ZQSZ, 2) AS DECIMAL(16, 2)) AS ZQSZ,
                       CAST(ROUND(ZJYE, 2) AS DECIMAL(16, 2)) AS ZJYE,
                       CAST(ROUND(ZTZC, 2) AS DECIMAL(16, 2)) AS ZTZC,
                       CAST(ROUND(QTZC, 2) AS DECIMAL(16, 2)) AS QTZC,
                       CAST(ROUND(CRJE, 2) AS DECIMAL(16, 2)) AS CRJE,
                       CAST(ROUND(QCJE, 2) AS DECIMAL(16, 2)) AS QCJE,
                       CAST(ROUND(ZRSZ, 2) AS DECIMAL(16, 2)) AS ZRSZ,
                       CAST(ROUND(ZCSZ, 2) AS DECIMAL(16, 2)) AS ZCSZ,
                       CAST(ROUND(ZFZ, 2) AS DECIMAL(16, 2)) AS ZFZ,
                       CAST(ROUND(ZFZ_SR, 2) AS DECIMAL(16, 2)) AS ZFZ_SR,
                       CAST(ROUND(CCCB, 2) AS DECIMAL(16, 2)) AS CCCB,
                       CAST(ROUND(DRYK, 2) AS DECIMAL(16, 2)) AS DRYK, " ||
                "      CAST(ROUND((CASE WHEN TZZH_FE = 0 THEN 0 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK WHEN ZZC_SR > 0 AND ZZC = 0 THEN 0 ELSE TZZH_FE END),2) AS DECIMAL(16,2)) AS TZZH_FE,  " ||
                "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_FE_SR END),2) AS DECIMAL(16,2)) AS TZZH_FE_SR,  " ||
                "      CAST(ROUND((CASE WHEN TZZH_ZXFE = 0 THEN 0 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK WHEN ZZC_SR > 0 AND ZZC = 0 THEN 0 ELSE TZZH_ZXFE END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE,  " ||
                "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN ZZC - ZFZ - DRYK ELSE TZZH_ZXFE_SR END),2) AS DECIMAL(16,2)) AS TZZH_ZXFE_SR,  " ||
                "      CAST(ROUND((CASE WHEN TZZH_ZXFE = 0 THEN 1 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10) WHEN ZZC_SR > 0 AND ZZC = 0 THEN 1 ELSE TZZH_ZXJZ END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ,  " ||
                "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE TZZH_ZXJZ_SR END),4) AS DECIMAL(10,4)) AS TZZH_ZXJZ_SR,  " ||
                "      CAST(ROUND((CASE WHEN TZZH_FE = 0 THEN 1 WHEN ZZC_SR = 0 AND ZZC > 0 THEN ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10) WHEN ZZC_SR > 0 AND ZZC = 0 THEN 1 ELSE TZZH_LJJZ END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ,  " ||
                "      CAST(ROUND((CASE WHEN ZZC_SR = 0 AND ZZC > 0 THEN 1.0 ELSE TZZH_LJJZ_SR END),4) AS DECIMAL(22,4)) AS TZZH_LJJZ_SR,  " ||
                "      CAST(ROUND(RZFZ, 2) AS DECIMAL(16, 2)) AS RZFZ,
                       CAST(ROUND(RQFZ, 2) AS DECIMAL(16, 2)) AS RQFZ,
                       CAST(ROUND(YJLX, 2) AS DECIMAL(16, 2)) AS YJLX,
                       CAST(ROUND(GHLX, 2) AS DECIMAL(16, 2)) AS GHLX,
                       CAST(0 AS DECIMAL(16, 2)) AS YJLX_SR,
                       CAST(ROUND(LXSR, 2) AS DECIMAL(16, 2)) AS LXSR,
                       CAST(ROUND(LXZC_QT, 2) AS DECIMAL(16, 2)) AS LXZC_QT,
                       CAST(ROUND(QTSR, 2) AS DECIMAL(16, 2)) AS QTSR,
                       CAST(ROUND(QTZC_FY, 2) AS DECIMAL(16, 2)) AS QTZC_FY,
                       CAST(ROUND(JYL, 2) AS DECIMAL(16, 2)) AS JYL,
                       CAST(ROUND(XZRQFZ, 2) AS DECIMAL(16, 2)) AS XZRQFZ,
                       CAST(ROUND(XZRQYJLX, 2) AS DECIMAL(16, 2)) AS XZRQYJLX,
                       CAST(ROUND(XZRZYJLX, 2) AS DECIMAL(16, 2)) AS XZRZYJLX,
                       CAST(ROUND(XZFZ, 2) AS DECIMAL(16, 2)) AS XZFZ,
                       CAST(ROUND(XZHKJE, 2) AS DECIMAL(16, 2)) AS XZHKJE,
                       CAST(ROUND(XZRQHKJE, 2) AS DECIMAL(16, 2)) AS XZRQHKJE,
                       CAST(ROUND(XZRZHKJE, 2) AS DECIMAL(16, 2)) AS XZRZHKJE,
                       CAST(ROUND(MQHQJE, 2) AS DECIMAL(16, 2)) AS MQHQJE,
                       CAST(ROUND(XZGHLX, 2) AS DECIMAL(16, 2)) AS XZGHLX,
                       CAST(ROUND(XZRQGHLX, 2) AS DECIMAL(16, 2)) AS XZRQGHLX,
                       CAST(ROUND(XZRZGHLX, 2) AS DECIMAL(16, 2)) AS XZRZGHLX,
                       CAST(ROUND(HKJE, 2) AS DECIMAL(16, 2)) AS HKJE,
                       CAST(ROUND(MQHKJE, 2) AS DECIMAL(16, 2)) AS MQHKJE,
                       " || I_RQ || " AS RQ
                  FROM (SELECT NVL(TZZH_ID, 0) AS TZZH_ID,
                               T.KHH,
                               NVL(ZZC_SR, 0) AS ZZC_SR,
                               NVL(ZZC, 0) AS ZZC,
                               NVL(ZQSZ, 0) AS ZQSZ,
                               NVL(ZJYE, 0) AS ZJYE,
                               NVL(ZTZC, 0) AS ZTZC,
                               NVL(QTZC, 0) AS QTZC,
                               NVL(CRJE, 0) AS CRJE,
                               NVL(QCJE, 0) AS QCJE,
                               NVL(ZRSZ, 0) AS ZRSZ,
                               NVL(ZCSZ, 0) AS ZCSZ,
                               NVL(ZFZ_SR, 0) AS ZFZ_SR,
                               NVL(ZFZ, 0) AS ZFZ,
                               NVL(CCCB, 0) AS CCCB,
                               NVL((ZZC - ZFZ) - (ZZC_SR - ZFZ_SR) - (CRJE - QCJE) - (ZRSZ - ZCSZ), 0) AS DRYK,
                               ROUND(NVL(TZZH_FE, 0), 2) AS TZZH_FE,
                               NVL(TZZH_FE_SR, 0) AS TZZH_FE_SR,
                               NVL(TZZH_ZXFE, 0) AS TZZH_ZXFE,
                               NVL(TZZH_ZXFE_SR, 0) AS TZZH_ZXFE_SR,
                               IF(NVL(TZZH_ZXFE, 0) = 0,
                                  1.0,
                                  ROUND((NVL(ZZC, 0) - NVL(ZFZ, 0)) / TZZH_ZXFE, 4)) AS TZZH_ZXJZ,
                               NVL(TZZH_ZXJZ_SR, 0) AS TZZH_ZXJZ_SR,
                               IF(NVL(TZZH_FE, 0) = 0,
                                  1.0,
                                  ROUND((NVL(ZZC, 0) - NVL(ZFZ, 0)) / TZZH_FE, 4)) AS TZZH_LJJZ,
                               NVL(TZZH_LJJZ_SR, 0) AS TZZH_LJJZ_SR,
                               NVL(RZFZ, 0) AS RZFZ,
                               NVL(RQFZ, 0) AS RQFZ,
                               NVL(YJLX, 0) AS YJLX,
                               NVL(GHLX, 0) AS GHLX,
                               NVL(LXSR, 0) AS LXSR,
                               NVL(LXZC_QT, 0) AS LXZC_QT,
                               NVL(QTSR, 0) AS QTSR,
                               NVL(QTZC_FY, 0) AS QTZC_FY,
                               NVL(JYL, 0) AS JYL,
                               NVL(XZRQFZ, 0) AS XZRQFZ,
                               NVL(XZRQYJLX, 0) AS XZRQYJLX,
                               NVL(XZRZYJLX, 0) AS XZRZYJLX,
                               NVL(XZFZ, 0) AS XZFZ,
                               NVL(XZHKJE, 0) AS XZHKJE,
                               NVL(XZRQHKJE, 0) AS XZRQHKJE,
                               NVL(XZRZHKJE, 0) AS XZRZHKJE,
                               NVL(MQHQJE, 0) AS MQHQJE,
                               NVL(XZGHLX, 0) AS XZGHLX,
                               NVL(XZRQGHLX, 0) AS XZRQGHLX,
                               NVL(XZRZGHLX, 0) AS XZRZGHLX,
                               NVL(HKJE, 0) AS HKJE,
                               NVL(MQHKJE, 0) AS MQHKJE
                          FROM (SELECT TZZH_ID,
                                       S.KHH,
                                       NVL(SUM(ZZC_SR), 0) AS ZZC_SR,
                                       NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +
                                       NVL(SUM(QTZC), 0) AS ZZC,
                                       NVL(SUM(ZQSZ), 0) AS ZQSZ,
                                       NVL(SUM(ZJYE), 0) AS ZJYE,
                                       NVL(SUM(ZTZC), 0) AS ZTZC,
                                       NVL(SUM(QTZC), 0) AS QTZC,
                                       NVL(SUM(CRJE), 0) AS CRJE,
                                       NVL(SUM(QCJE), 0) AS QCJE,
                                       NVL(SUM(ZRSZ), 0) AS ZRSZ,
                                       NVL(SUM(ZCSZ), 0) AS ZCSZ,
                                       NVL(SUM(ZFZ_SR), 0) AS ZFZ_SR,
                                       NVL(SUM(ZFZ), 0) AS ZFZ,
                                       NVL(SUM(RZFZ), 0) AS RZFZ,
                                       NVL(SUM(RQFZ), 0) AS RQFZ,
                                       NVL(SUM(YJLX), 0) AS YJLX,
                                       NVL(SUM(YJLX_SR), 0) AS YJLX_SR,
                                       NVL(SUM(GHLX), 0) AS GHLX,
                                       NVL(SUM(CCCB), 0) AS CCCB,
                                       CASE
                                           WHEN NVL(SUM(TZZH_LJJZ_SR), 1.0) = 0 THEN
                                           --如果上日累计净值为0，取本日资产作为份额
                                            NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +
                                            NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ), 0)
                                           ELSE
                                            IF(SUM(TZZH_ZXFE_SR) IS NULL,
                                               NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +
                                               NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ), 0),
                                               NVL(SUM(TZZH_FE_SR), 0) +
                                               (NVL(SUM(CRJE),0) + NVL(SUM(LXSR),0) - NVL(SUM(QCJE),0) -
                                       NVL(SUM(LXZC_QT), 0) + NVL(SUM(QTSR), 0) - NVL(SUM(QTZC_FY), 0) + NVL(SUM(ZRSZ),0) - (NVL(SUM(ZCSZ), 0)-NVL(SUM(ZCSZ_YK),0)) +
                                       NVL(SUM(HLXZ), 0)) / NVL(SUM(TZZH_LJJZ_SR), 1.0))
                                       END AS TZZH_FE,
                                       NVL(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR,
                                       NVL(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR,
                                       CASE
                                           WHEN NVL(SUM(TZZH_ZXJZ_SR), 1.0) = 0 THEN
                                           --如果上日累计净值为0，取本日资产作为最新份额
                                            NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +
                                            NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ), 0)
                                           ELSE
                                            IF(SUM(TZZH_ZXFE_SR) IS NULL,
                                               NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) + NVL(SUM(ZTZC), 0) +
                                               NVL(SUM(QTZC), 0) - NVL(SUM(ZFZ), 0),
                                               NVL(SUM(TZZH_ZXFE_SR), 0) +
                                               (NVL(SUM(CRJE),0) + NVL(SUM(LXSR),0) - NVL(SUM(QCJE),0) -
                                       NVL(SUM(LXZC_QT), 0) + NVL(SUM(QTSR),0) - NVL(SUM(QTZC_FY), 0) + NVL(SUM(ZRSZ),0) - (NVL(SUM(ZCSZ), 0)-NVL(SUM(ZCSZ_YK),0)) +
                                       NVL(SUM(HLXZ), 0)) / NVL(SUM(TZZH_ZXJZ_SR), 1.0))
                                       END AS TZZH_ZXFE,
                                       NVL(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR,
                                       NVL(SUM(TZZH_ZXJZ_SR), 0) AS TZZH_ZXJZ_SR,
                                       NVL(SUM(LXSR), 0) AS LXSR,
                                       NVL(SUM(LXZC_QT), 0) AS LXZC_QT,
                                       NVL(SUM(QTSR), 0) AS QTSR,
                                       NVL(SUM(QTZC_FY), 0) AS QTZC_FY,
                                       NVL(SUM(JYL), 0) AS JYL,
                                       NVL(SUM(XZRQFZ), 0) AS XZRQFZ,
                                       NVL(SUM(XZRQYJLX), 0) AS XZRQYJLX,
                                       NVL(SUM(XZRZYJLX), 0) AS XZRZYJLX,
                                       NVL(SUM(XZFZ), 0) AS XZFZ,
                                       NVL(SUM(XZHKJE), 0) AS XZHKJE,
                                       NVL(SUM(XZRQHKJE), 0) AS XZRQHKJE,
                                       NVL(SUM(XZRZHKJE), 0) AS XZRZHKJE,
                                       NVL(SUM(MQHQJE), 0) AS MQHQJE,
                                       NVL(SUM(XZGHLX), 0) AS XZGHLX,
                                       NVL(SUM(XZRQGHLX), 0) AS XZRQGHLX,
                                       NVL(SUM(XZRZGHLX), 0) AS XZRZGHLX,
                                       NVL(SUM(HKJE), 0) AS HKJE,
                                       NVL(SUM(MQHKJE), 0) AS MQHKJE
                                  FROM " || F_IDS_GET_TABLENAME('xy_sparkCustDaily', I_KHH) || " S
                                 GROUP BY TZZH_ID, S.KHH) T
                          LEFT JOIN CUST.T_KHXX_JJYW K
                            ON (T.KHH = K.KHH)
                         WHERE NOT (K.XHRQ IS NOT NULL AND K.XHRQ < " || I_RQ || ")) A";

     F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

     F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 'T_STAT_XY_ZC_R', I_RQ, I_KHH);
  END;

END;

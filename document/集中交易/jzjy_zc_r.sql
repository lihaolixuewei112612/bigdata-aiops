create or replace procedure cust.p_ids_cust_daily_stat(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_CUST_DAILY_STAT
  *项目名称：IDS计算
  *文件说明：集中交易-资产日统计

  创建人：胡阳明
  功能说明：集中交易-资产日统计

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/6/21       创建
  燕居庆        v1.0.1        2019/8/19       根据Java内容调整sql脚本
                                                1.增加zqsz_flt_sr
                                                2.股票质押数据取DSC_BAS.T_GPZY_DHGHY_HIS
                                                3.金融产品持仓成本计算
                                                4.调整资产修正，增加抵押品市值修正
                                                5.资金存取调整
                                                6.去除分红扩股计算
                                                7.资产计算减去zqsz_flt
  燕居庆        v1.0.2        2019/9/25        对标java-ids 5051版本
                                                1.转出市值-盈亏修正
  燕居庆        v1.0.3        2019/9/27        对标java-ids 5080版本
                                                1.最新净值=总资产-总负债
 燕居庆          v1.0.4        2019/11/15       增加其他收入、其他支出
 燕居庆          v1.0.5        2020/2/25        证券余额市值去除证券类别的关联
 燕居庆          v1.0.6        2020/2/25        计算当日盈亏时，减去总负债
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING;
l_sqlWhereLastDay STRING;
l_lastDay INT;
l_sql STRING;
l_khh STRING;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
BEGIN
  BEGIN
    --1.1 获取汇率参数
    SELECT CAST(F_GET_HLCS('2',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsHKD FROM `SYSTEM`.dual;
    SELECT CAST(F_GET_HLCS('3',I_RQ) AS DECIMAL(12,6)) INTO l_hlcsUSD FROM `SYSTEM`.dual;
  END;
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;


  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
    l_khh:='';
  ELSE
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_khh :=' and khh='||I_KHH;
  END IF;



  BEGIN
  -------------------------------------------金融产品投资损益---------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkFpTzsy', I_KHH);
    --金融产品盈亏分析:P_IDS_FINANCE_PRODUCT_COST.sql
    l_sqlBuf := "select * from cust.t_fp_tzsy where qcrq = " || l_sqlWhereCurrentDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  -------------------------------------------集中交易清仓-------------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkTzsy', I_KHH);
    l_sqlBuf := "select * from cust.t_tzsy where qcrq = " || l_sqlWhereCurrentDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  ------------------------------------------集中交易交割明细流水-----------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH);
    l_sqlBuf := "select * from cust.t_jgmxls_his where cjrq = " || l_sqlWhereCurrentDay;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  -------------------------------------------集中交易资金流水--------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkZjmxls', I_KHH);
    l_sqlBuf := "select *,0 as tzzh_id from dsc_bas.t_zjmxls_his where rq = " || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  -------------------------------------------集中交易-成本计算-------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeCbjs', I_KHH);
    l_sqlBuf := "select * from CUST.T_ZQYE_CBJS where rq = " || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  -------------------------------------------金融产品成本计算--------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkFpCbjs', I_KHH);
    l_sqlBuf := "select * from CUST.T_FP_CPFE_CBJS where rq = " || l_sqlWhereCurrentDay;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  ------------------------------------------资产修正---------------------------------------------
    l_tableName := F_IDS_GET_TABLENAME('sparkZcxz', I_KHH);
    l_sqlBuf := "SELECT * FROM DSC_STAT.T_STAT_KHZCXZMX WHERE  ZHLB IN ('1', '3') AND " || I_RQ || " BETWEEN KSRQ AND JSRQ " || l_khh;

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;



------------------------------------//1、生成集中交易资金余额数据------------------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkStatDR', I_KHH);
    l_sqlBuf :=
                "SELECT  TZZH_ID," ||
                "        KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "        cast(0 AS decimal(16,2)) AS zqsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        CAST(SUM(CASE WHEN BZ = '2' THEN ZHYE * " || l_hlcsHKD ||
                "                 WHEN BZ = '3' THEN ZHYE * " || l_hlcsUSD ||
                "                 ELSE ZHYE" ||
                "            END) AS DECIMAL(16,2)) AS ZJYE," ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS cccb,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,     " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                " FROM CUST.T_ZJYE_HIS T WHERE RQ = " || l_sqlWhereCurrentDay ||
                " GROUP BY TZZH_ID, KHH";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

-------------------------------------//获取字段----------------------------------------------
  BEGIN
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
 ------------------------------------//2、生成证券市值数据----------------------------------------------------
  BEGIN
    l_sqlBuf :=
                "SELECT TZZH_ID," ||
                "       KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "       CAST(SUM(CASE" ||
                "             WHEN T.BZ = '2' THEN" ||
                "              T.ZXSZ * " || l_hlcsHKD ||
                "             WHEN T.BZ = '3' THEN" ||
                "              T.ZXSZ * " || l_hlcsUSD ||
                "             ELSE" ||
                "              T.ZXSZ" ||
                "           END) AS DECIMAL(16,2)) AS ZQSZ," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "       CAST(SUM(CASE" ||
                "             WHEN T.BZ = '2' THEN" ||
                "              nvl(CCCB, cast(0 AS decimal(16,2))) * " || l_hlcsHKD ||
                "             WHEN T.BZ = '3' THEN" ||
                "              nvl(CCCB, cast(0 AS decimal(16,2))) * " || l_hlcsUSD ||
                "             ELSE" ||
                "              nvl(CCCB, cast(0 AS decimal(16,2)))" ||
                "           END) AS DECIMAL(16,2)) AS CCCB," ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,     " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "  FROM (select * from CUST.T_ZQYE_HIS WHERE RQ = " || l_sqlWhereCurrentDay || ") T " ||
                " GROUP BY TZZH_ID, KHH";


 -- 数据插入至资产统计临时表sparkStatDR
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;
-------------------------------------//2-1 非流通证券市值数据------------------------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT 0 as TZZH_ID," ||
                "       KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc, " ||
                "        cast(0 AS decimal(16,2)) as zqsz,        " ||
                "       CAST(SUM(CASE" ||
                "             WHEN BZ = '2' THEN" ||
                "              ZXSZ * " || l_hlcsHKD ||
                "             WHEN BZ = '3' THEN" ||
                "              ZXSZ * " || l_hlcsUSD ||
                "             ELSE" ||
                "              ZXSZ" ||
                "           END) AS DECIMAL(16,2)) AS zqsz_flt," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp," ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS CCCB,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,     " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "  FROM dsc_bas.t_zqye_flt_his T  where RQ =  " || l_sqlWhereCurrentDay ||
                "  GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

-------------------------------------//3、生成金融产品份额数据-----------------------------------------------------------
  BEGIN
    l_sqlBuf :=
                "SELECT cast(0 AS decimal(16,2)) AS TZZH_ID," ||
                "       KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "       CAST(SUM(CASE" ||
                "             WHEN BZ = '2' THEN" ||
                "              ZXSZ * " || l_hlcsHKD ||
                "             WHEN BZ = '3' THEN" ||
                "              ZXSZ * " || l_hlcsUSD ||
                "             ELSE" ||
                "              ZXSZ" ||
                "           END) AS DECIMAL(16,2)) AS ZQSZ," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        CAST(SUM(CASE" ||
                "             WHEN BZ = '2' THEN" ||
                "              NVL(CCCB, 0) * " || l_hlcsHKD ||
                "             WHEN BZ = '3' THEN" ||
                "              NVL(CCCB, 0) * " || l_hlcsUSD ||
                "             ELSE" ||
                "              NVL(CCCB, 0)" ||
                "           END) AS DECIMAL(16,2)) AS CCCB," ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "       CAST(SUM(CASE" ||
                "             WHEN BZ = '2' THEN" ||
                "              ZXSZ * " || l_hlcsHKD ||
                "             WHEN BZ = '3' THEN" ||
                "              ZXSZ * " || l_hlcsUSD ||
                "             ELSE" ||
                "              ZXSZ" ||
                "           END) AS DECIMAL(16,2)) AS ZQSZ_JRCP," ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,     " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "  FROM CUST.T_FP_CPFE_HIS where rq = " || l_sqlWhereCurrentDay ||
                "  GROUP BY KHH";


    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

-------------------------------------//4、处理在途资产等修正数据------------------------------------------------------
  BEGIN
    l_sqlBuf :=
                "SELECT 0 as TZZH_ID," ||
                "       KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "        cast(0 AS decimal(16,2)) AS zqsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "         CAST(SUM(CASE" ||
                "             WHEN XZLB IN ('1') THEN" ||
                --//在途资产(主要指交收规则引起的未入资产)
                "              XZJE * CASE BZ" ||
                "                WHEN '2' THEN" ||
                "                 "|| l_hlcsHKD ||
                "                WHEN '3' THEN" ||
                "                 " || l_hlcsUSD ||
                "                ELSE" ||
                "                 1" ||
                "              END" ||
                "             ELSE" ||
                "              0" ||
                "           END) AS DECIMAL(16,2)) AS zqsz_dyp," ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "       CAST(SUM(CASE" ||
                "             WHEN XZLB IN ('11', '12', '13', '14', '15', '22', '23','20','21') THEN" ||
                --//在途资产(主要指交收规则引起的未入资产)
                "              XZJE * CASE BZ" ||
                "                WHEN '2' THEN" ||
                "                 " || l_hlcsHKD ||
                "                WHEN '3' THEN" ||
                "                 " || l_hlcsUSD ||
                "                ELSE" ||
                "                 1" ||
                "              END" ||
                "             ELSE" ||
                "              cast(0 AS decimal(16,2))" ||
                "           END) AS DECIMAL(16,2)) AS ZTZC," ||
                "       CAST(SUM(CASE" ||
                "             WHEN XZLB IN ('6', '7', '3', '31', '32', '33', '34') THEN" ||
                --//1、4：抵押品市值（包括：质押入库债券市值,  股票质押回购市值）
                --//3： 债权资产(包括：质押式回购融券，报价回购融资，转融通出借)
                --//31、32、33、34：市值修正金额(主要包括分红扩股、配股的修正)
                "              XZJE * CASE BZ" ||
                "                WHEN '2' THEN" ||
                "                 " || l_hlcsHKD ||
                "                WHEN '3' THEN" ||
                "                 " || l_hlcsUSD ||
                "                ELSE" ||
                "                 1" ||
                "              END" ||
                "             ELSE" ||
                "              cast(0 AS decimal(16,2))" ||
                "           END) AS DECIMAL(16,2)) AS QTZC," ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "       CAST(SUM(CASE" ||
                "             WHEN XZLB IN ('2', '5') THEN" ||
                --//总负债：这里取值为 质押式回购融资，股票质押回购融资金额
                "              XZJE * CASE BZ" ||
                "                WHEN '2' THEN" ||
                "                 " || l_hlcsHKD ||
                "                WHEN '3' THEN" ||
                "                 " || l_hlcsUSD ||
                "                ELSE" ||
                "                 1" ||
                "              END" ||
                "             ELSE" ||
                "              cast(0 AS decimal(16,2))" ||
                "           END) AS DECIMAL(16,2)) AS ZFZ," ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS cccb,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,     " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "  FROM   " || F_IDS_GET_TABLENAME('sparkZcxz', I_KHH) || "  T " ||
                "  GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

-------------------------------------//5、生成存入资金、取出资金数据------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT 0 AS TZZH_ID," ||
                "        khh," ||
                "        0 AS zzc_sr,      " ||
                "        0 AS zzc,         " ||
                "        0 AS zqsz,        " ||
                "        0 AS zqsz_flt,    " ||
                "        0 AS zqsz_dyp,    " ||
                "        0 AS zjye,        " ||
                "        0 AS ztzc,        " ||
                "        0 AS qtzc,        " ||
                "       CAST(SUM(CASE WHEN SJKM = '101' " ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS CRJE," ||
                "       CAST(SUM(CASE WHEN SJKM = '102' " ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS QCJE," ||
                "        0 AS zrsz,        " ||
                "        0 AS zcsz,        " ||
                "        0 AS zfz,         " ||
                "        0 AS zfz_sr,      " ||
                "        0 AS cccb,        " ||
                "        0 AS dryk,        " ||
                "        0 AS tzzh_fe,     " ||
                "        0 AS tzzh_fe_sr,  " ||
                "        0 AS tzzh_zxfe,   " ||
                "        0 AS tzzh_zxfe_sr," ||
                "        0 AS tzzh_zxjz,   " ||
                "        0 AS tzzh_zxjz_sr," ||
                "        0 AS tzzh_ljjz,   " ||
                "        0 AS tzzh_ljjz_sr," ||
                "        0 AS zqsz_jrcp,   " ||
                "        CAST(SUM(CASE WHEN sjkm IN ('105')" ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS LXSR," ||
                "        0 AS ZCZJ_TOTC,   " ||
                "        0 AS ZRZJ_FOTC,   " ||
                "        0 AS lxzc_gpzy,   " ||
                "        0 AS jyfy_gpzy,   " ||
                "        0 AS LXZC_QT," ||
                "         CAST(SUM(CASE WHEN sjkm in ('111')" ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS QTSR," ||
                "         CAST(SUM(CASE WHEN sjkm in ('112')" ||
                "                    THEN fsje * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS QTZC_FY," ||
                "        0 AS jyl,          " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                " FROM DSC_STAT.T_STAT_ZJCQK_R T WHERE T.ZHLB = '1' AND T.RQ = " || l_sqlWhereCurrentDay ||  --DSC_STAT.T_STAT_ZJCQK_R<--P_STAT_ZJCQK_R.sql
                " GROUP BY khh";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

-------------------------------------//6、生成市值转入、市值转出数据------------------------------------------
  BEGIN
    l_sqlBuf :="SELECT 0 AS TZZH_ID," ||
                "        khh," ||
                "        0 AS zzc_sr,      " ||
                "        0 AS zzc,         " ||
                "        0 AS zqsz,        " ||
                "        0 AS zqsz_flt," ||
                "        0 AS zqsz_dyp," ||
                "        0 AS zjye,        " ||
                "        0 AS ztzc,        " ||
                "        0 AS qtzc,        " ||
                "        0 AS crje,        " ||
                "        0 AS qcje,        " ||
                "       CAST(SUM(CASE WHEN cqfx = '1'" ||
                "                    THEN zqsz * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS ZRSZ," ||
                "       CAST(SUM(CASE WHEN cqfx = '2'" ||
                "                    THEN zqsz * CASE BZ " ||
                "                                WHEN '2' THEN " || l_hlcsHKD ||
                "                                WHEN '3' THEN " || l_hlcsUSD ||
                "                                ELSE" ||
                "                                 1 " ||
                "                               END" ||
                "                    ELSE 0" ||
                "                END) AS DECIMAL(16,2)) AS ZCSZ," ||
                "        0 AS zfz,         " ||
                "        0 AS zfz_sr,      " ||
                "        0 AS cccb,        " ||
                "        0 AS dryk,        " ||
                "        0 AS tzzh_fe,     " ||
                "        0 AS tzzh_fe_sr,  " ||
                "        0 AS tzzh_zxfe,   " ||
                "        0 AS tzzh_zxfe_sr," ||
                "        0 AS tzzh_zxjz,   " ||
                "        0 AS tzzh_zxjz_sr," ||
                "        0 AS tzzh_ljjz,   " ||
                "        0 AS tzzh_ljjz_sr," ||
                "        0 AS zqsz_jrcp,   " ||
                "        0 AS lxsr,        " ||
                "        0 AS zczj_totc,   " ||
                "        0 AS zrzj_fotc,   " ||
                "        0 AS lxzc_gpzy,   " ||
                "        0 AS jyfy_gpzy,   " ||
                "        0 AS lxzc_qt,     " ||
                "        0 AS qtsr,        " ||
                "        0 AS qtzc_fy,     " ||
                "        0 AS JYL,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                " FROM DSC_STAT.T_STAT_ZQZRZC_R T WHERE T.ZHLB IN ('1', '3') AND T.RQ = " || l_sqlWhereCurrentDay ||
                " GROUP BY khh";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;


 ------------------------------------//9、佣金返还------------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT 0 AS TZZH_ID," ||
                "        KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "        cast(0 AS decimal(16,2)) AS zqsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        CAST(SUM(QRFE) AS DECIMAL(16,2)) AS ZRSZ," ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS cccb,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "    FROM cust.T_FP_JGMXLS_HIS  " ||
                "   WHERE YWDM = '134'" ||
                "   AND QRRQ = " || l_sqlWhereCurrentDay ||
                "   GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;
 ------------------------------------//10、股票质押利息支出：初始交易贷款到的金额需要扣除费用，即扣除费用后所贷到的金额才是真正后续资金使用额-------------------
  BEGIN
    l_sqlBuf := "SELECT 0 AS TZZH_ID," ||
                "         KHH," ||
                "        cast(0 AS decimal(16,2)) AS zzc_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "        cast(0 AS decimal(16,2)) AS zqsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "        cast(0 AS decimal(16,2)) AS zfz_sr,      " ||
                "        cast(0 AS decimal(16,2)) AS cccb,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe_sr,  " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe_sr," ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz,   " ||
                "        cast(0 AS decimal(16,10)) AS tzzh_zxjz_sr," ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz,   " ||
                "        cast(0 AS decimal(22,4)) AS tzzh_ljjz_sr," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "         CAST(SUM(jsje - cjje) AS DECIMAL(16,2)) AS LXZC_GPZY," ||  --利息支出
                "         CAST(SUM(CASE WHEN CJJE != 0 AND NVL(YSJE, 0) != 0 THEN CJJE - ABS(YSJE) ELSE 0 END) AS DECIMAL(16,2)) AS JYFY_GPZY," ||  --交易费用
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "    FROM DSC_BAS.T_GPZY_DHGHY_HIS T " ||
                "   WHERE T.jsrq = " || I_RQ  ||
                "   AND T.RQ = " || l_sqlWhereCurrentDay ||
                "   GROUP BY KHH ";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
     EXECUTE IMMEDIATE l_sql;
  END;

-------------------------------------//11、上日数据----------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT TZZH_ID," ||
                "         KHH," ||
                "         ZZC       AS ZZC_SR," ||
                "        cast(0 AS decimal(16,2)) AS zzc,         " ||
                "        cast(0 AS decimal(16,2)) AS zqsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_flt,    " ||
                "        cast(0 AS decimal(16,2)) AS zqsz_dyp,    " ||
                "        cast(0 AS decimal(16,2)) AS zjye,        " ||
                "        cast(0 AS decimal(16,2)) AS ztzc,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc,        " ||
                "        cast(0 AS decimal(16,2)) AS crje,        " ||
                "        cast(0 AS decimal(16,2)) AS qcje,        " ||
                "        cast(0 AS decimal(16,2)) AS zrsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz,        " ||
                "        cast(0 AS decimal(16,2)) AS zfz,         " ||
                "         ZFZ       AS ZFZ_SR," ||
                "        cast(0 AS decimal(16,2)) AS cccb,        " ||
                "        cast(0 AS decimal(16,2)) AS dryk,        " ||
                "        cast(0 AS decimal(16,2)) AS tzzh_fe,     " ||
                "         TZZH_FE   AS TZZH_FE_SR," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxfe,   " ||
                "         TZZH_ZXFE AS TZZH_ZXFE_SR," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_zxjz,   " ||
                "         TZZH_ZXJZ AS TZZH_ZXJZ_SR," ||
                "        cast(0 AS decimal(16,2)) AS tzzh_ljjz,   " ||
                "         TZZH_LJJZ AS TZZH_LJJZ_SR," ||
                "        cast(0 AS decimal(16,2)) AS zqsz_jrcp,   " ||
                "        cast(0 AS decimal(16,2)) AS lxsr,        " ||
                "        cast(0 AS decimal(16,2)) AS zczj_totc,   " ||
                "        cast(0 AS decimal(16,2)) AS zrzj_fotc,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS jyfy_gpzy,   " ||
                "        cast(0 AS decimal(16,2)) AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        cast(0 AS decimal(16,2)) AS jyl,         " ||
                "        cast(0 AS decimal(16,2)) AS hlxz,        " ||
                "        cast(0 AS decimal(16,2)) AS zcsz_yk      " ||
                "    FROM cust.t_stat_ZC_R T where rq = " || l_sqlWhereLastDay;

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
     EXECUTE IMMEDIATE l_sql;
  END;

  ---------------------------------------无持仓红利的情况--------------------------------------------
  BEGIN
    l_sqlBuf := "SELECT 0 AS TZZH_ID," ||
                "        KHH," ||
                "        0 AS zzc_sr,      " ||
                "        0 AS zzc,         " ||
                "        0 AS zqsz,        " ||
                "        0 AS zqsz_flt," ||
                "        0 AS zqsz_dyp," ||
                "        0 AS zjye,        " ||
                "        0 AS ztzc,        " ||
                "        0 AS qtzc,        " ||
                "        0 AS crje,        " ||
                "        0 AS qcje,        " ||
                "        0 AS zrsz,        " ||
                "        0 AS zcsz,        " ||
                "        0 AS zfz,         " ||
                "        0 AS zfz_sr,      " ||
                "        0 AS cccb,        " ||
                "        0 AS dryk,        " ||
                "        0 AS tzzh_fe,     " ||
                "        0 AS tzzh_fe_sr,  " ||
                "        0 AS tzzh_zxfe,   " ||
                "        0 AS tzzh_zxfe_sr," ||
                "        0 AS tzzh_zxjz,   " ||
                "        0 AS tzzh_zxjz_sr," ||
                "        0 AS tzzh_ljjz,   " ||
                "        0 AS tzzh_ljjz_sr," ||
                "        0 AS zqsz_jrcp,   " ||
                "        0 AS lxsr,        " ||
                "        0 AS zczj_totc,   " ||
                "        0 AS zrzj_fotc,   " ||
                "        0 AS LXZC_GPZY," ||
                "        0 AS JYFY_GPZY," ||
                "        0 AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        0 AS jyl,         " ||
                "        SUM(HLXZ) AS hlxz," ||
                "        0 AS zcsz_yk" ||
                " FROM " ||
                " (SELECT " ||
                "        KHH," ||
                "        CAST(CASE WHEN BZ = '2' THEN YSJE * " || l_hlcsHKD ||
                "                 WHEN BZ = '3' THEN YSJE * " || l_hlcsUSD ||
                "                 ELSE YSJE" ||
                "            END AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) || " J " ||
                "  WHERE NOT EXISTS " ||
                "       (SELECT 1 FROM " ||
                "            (SELECT KHH,GDH,JYS,ZQDM FROM CUST.T_ZQYE_HIS WHERE RQ = "|| l_sqlWhereCurrentDay ||
                "           UNION ALL" ||
                "           SELECT KHH,GDH,JYS,ZQDM FROM " || F_IDS_GET_TABLENAME('sparkTzsy', I_KHH) ||") A" ||
                "         WHERE J.KHH=A.KHH AND J.GDH=A.GDH AND J.JYS=A.JYS AND J.ZQDM=A.ZQDM)" ||
                "    AND J.JYLB='6'" ||
                " UNION ALL " ||
                " SELECT" ||
                "        KHH," ||
                "        CAST(CASE WHEN BZ = '2' THEN YSJE * " ||l_hlcsHKD ||
                "                 WHEN BZ = '3' THEN YSJE * " || l_hlcsUSD ||
                "                 ELSE YSJE" ||
                "            END AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM (SELECT j.KHH,j.GDH,j.JYS,j.ZQDM,j.bz,SUM(j.YSSL*NVL(h.ZXJ, 0)) YSJE,SUM(YSSL) YSSL FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) || " j left join (select * from dsc_bas.t_zqhq_xz_his where rq = " || I_RQ || ")
                h on (j.jys=h.jys and j.zqdm=h.zqdm) WHERE j.JYLB='16' AND j.YSSL>0 GROUP BY j.KHH,j.GDH,j.JYS,j.ZQDM,j.bz) J" ||
                "  WHERE EXISTS(SELECT 1 FROM " || F_IDS_GET_TABLENAME('sparkZqyeCbjs', I_KHH) || " A WHERE j.khh=a.khh and j.GDH=a.GDH and j.JYS=a.JYS and j.ZQDM=a.ZQDM and j.yssl=a.zqsl and j.yssl=a.dryssl)" ||
                " UNION ALL " ||
                " SELECT" ||
                "        KHH," ||
                "        CAST(QRJE AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM (SELECT j.KHH,j.JRCPZH,j.JRJGDM,j.CPDM,SUM(j.QRFE*NVL(h.zxjz,1)) QRJE,sum(QRFE) QRFE FROM  (select * from cust.T_FP_JGMXLS_HIS where qrrq = "|| l_sqlWhereCurrentDay ||") j left join (SELECT * FROM DSC_BAS.T_JRCPHQ_HIS WHERE RQ = " || I_RQ || ") h on (j.jrjgdm=h.jrjgdm and j.cpdm=h.cpdm) WHERE j.YWDM='143' AND j.QRFE>0 GROUP BY j.KHH,j.JRCPZH,j.JRJGDM,j.CPDM) J" ||
                "  WHERE EXISTS(SELECT 1 FROM " || F_IDS_GET_TABLENAME('sparkFpCbjs', I_KHH) || " A WHERE j.khh=a.khh and j.jrcpzh=a.jrcpzh and j.jrjgdm=a.jrjgdm and j.cpdm=a.cpdm and j.qrfe=a.cpsl and j.qrfe=a.dryssl)" ||
                " UNION ALL" ||
                " SELECT " ||
                "        KHH," ||
                "        CAST(SRJE AS DECIMAL(16,2)) AS hlxz" ||
                "   FROM " || F_IDS_GET_TABLENAME('sparkZjmxls', I_KHH) || " z WHERE YWKM='14201' AND ZJLY NOT IN ('SH', 'SZ') " ||
                "    AND NOT EXISTS(SELECT 1 FROM " ||
                "                       (SELECT KHH,JRJGDM,CPDM FROM CUST.T_FP_CPFE_HIS WHERE RQ = "|| l_sqlWhereCurrentDay ||
                "                       UNION ALL" ||
                "                       SELECT KHH,JRJGDM,CPDM FROM " || F_IDS_GET_TABLENAME('sparkFpTzsy', I_KHH) || ") A" ||
                "            WHERE Z.KHH=A.KHH AND Z.ZJLY=A.JRJGDM AND Z.XGPZ=A.CPDM)" ||
                " ) A" ||
                " GROUP BY KHH";


    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;


  END;

  -----------------------------------------------转出市值-盈亏修正值---------------------------------------------
  BEGIN

    l_sqlBuf := "SELECT 0 AS TZZH_ID," ||
                "        KHH," ||
                "        0 AS zzc_sr,      " ||
                "        0 AS zzc,         " ||
                "        0 AS zqsz,        " ||
                "        0 AS zqsz_flt," ||
                "        0 AS zqsz_dyp," ||
                "        0 AS zjye,        " ||
                "        0 AS ztzc,        " ||
                "        0 AS qtzc,        " ||
                "        0 AS crje,        " ||
                "        0 AS qcje,        " ||
                "        0 AS zrsz,        " ||
                "        0 AS zcsz,        " ||
                "        0 AS zfz,         " ||
                "        0 AS zfz_sr,      " ||
                "        0 AS cccb,        " ||
                "        0 AS dryk,        " ||
                "        0 AS tzzh_fe,     " ||
                "        0 AS tzzh_fe_sr,  " ||
                "        0 AS tzzh_zxfe,   " ||
                "        0 AS tzzh_zxfe_sr," ||
                "        0 AS tzzh_zxjz,   " ||
                "        0 AS tzzh_zxjz_sr," ||
                "        0 AS tzzh_ljjz,   " ||
                "        0 AS tzzh_ljjz_sr," ||
                "        0 AS zqsz_jrcp,   " ||
                "        0 AS lxsr,        " ||
                "        0 AS zczj_totc,   " ||
                "        0 AS zrzj_fotc,   " ||
                "        0 AS LXZC_GPZY," ||
                "        0 AS JYFY_GPZY," ||
                "        0 AS lxzc_qt,     " ||
                "        cast(0 AS decimal(16,2)) AS qtsr,        " ||
                "        cast(0 AS decimal(16,2)) AS qtzc_fy,     " ||
                "        0 AS jyl,         " ||
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
                " FROM (SELECT KHH,BZ,ZQDM,SUM(ZQSL) ZQSL FROM  DSC_STAT.T_STAT_ZQZRZC_R WHERE rq = "|| l_sqlWhereCurrentDay || " AND  ZHLB='1' AND ZQSZ > 0 GROUP BY KHH,BZ,ZQDM) Z," ||
                "      (SELECT KHH,BZ,ZQDM,SUM(ZQSL) ZQSL,SUM(LJYK) LJYK,SUM(LJMCJE) LJMCJE FROM " || F_IDS_GET_TABLENAME('sparkZqyeCbjs', I_KHH) ||" GROUP BY KHH,BZ,ZQDM) C" ||
                " WHERE Z.KHH=C.KHH AND Z.ZQDM=C.ZQDM AND Z.BZ=C.BZ" ||
                " UNION ALL" ||
                "  SELECT" ||
                "       Z.KHH," ||
                "       Z.BZ," ||
                "       Z.ZQSL/(Z.ZQSL+C.ZQSL)*C.LJYK AS ZCSZ_YK" ||
                " FROM (SELECT KHH,BZ,ZQDM,SUM(ZQSL) ZQSL FROM DSC_STAT.T_STAT_ZQZRZC_R WHERE rq = "|| l_sqlWhereCurrentDay || " AND ZHLB='3' AND ZQSZ > 0 GROUP BY KHH,BZ,ZQDM) Z," ||
                "      (SELECT KHH,BZ,CPDM,SUM(CPSL) ZQSL,SUM(LJYK) LJYK,SUM(LJMCJE) LJMCJE FROM "|| F_IDS_GET_TABLENAME('sparkFpCbjs', I_KHH) || " GROUP BY KHH,BZ,CPDM) C" ||
                " WHERE Z.KHH=C.KHH AND Z.ZQDM=C.CPDM AND Z.BZ=C.BZ) a GROUP BY KHH";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;
-------------------------------------//12、生成当日统计最终数据---------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkStatDRResult', I_KHH);
    l_sqlBuf :=
                "SELECT TZZH_ID," ||
                "         KHH," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK" ||
                "                ELSE" ||
                "                 ZZC_SR" ||
                "              END) AS DECIMAL(16, 2)) AS ZZC_SR," ||
                "         ZZC," ||
                "         ZQSZ," ||
                "         zqsz_flt,    " ||
                "         zqsz_dyp,    " ||
                "         ZJYE," ||
                "         ZTZC," ||
                "         QTZC," ||
                "         CRJE," ||
                "         QCJE," ||
                "         ZRSZ," ||
                "         ZCSZ," ||
                "         ZFZ," ||
                "         ZFZ_SR," ||
                "         CCCB," ||
                "         DRYK," ||
                "         CAST((CASE" ||
                "                WHEN TZZH_FE = 0 THEN" ||
                "                 0" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK - ZFZ" ||
                "                WHEN ZZC_SR > 0 AND ZZC = 0 THEN " ||
                "                 0" ||
                "                ELSE" ||
                "                 TZZH_FE" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_FE," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK - ZFZ" ||
                "                ELSE" ||
                "                 TZZH_FE_SR" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_FE_SR," ||
                "         CAST((CASE" ||
                "                WHEN TZZH_ZXFE = 0 THEN" ||
                "                 0" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK - ZFZ " ||
                "                WHEN ZZC_SR > 0 AND ZZC = 0 THEN " ||
                "                 0" ||
                "                ELSE" ||
                "                 TZZH_ZXFE" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_ZXFE," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ZZC - DRYK - ZFZ " ||
                "                ELSE" ||
                "                 TZZH_ZXFE_SR" ||
                "              END) AS DECIMAL(16, 2)) AS TZZH_ZXFE_SR," ||
                "         CAST((CASE" ||
                "                WHEN TZZH_ZXFE = 0 THEN" ||
                "                 1" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10)" ||
                "                WHEN ZZC_SR > 0 AND ZZC = 0 THEN " ||
                "                 1" ||
                "                ELSE" ||
                "                 TZZH_ZXJZ" ||
                "              END) AS DECIMAL(16, 10)) AS TZZH_ZXJZ," ||
                "         CAST((CASE" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 1.0" ||
                "                ELSE" ||
                "                 TZZH_ZXJZ_SR" ||
                "              END) AS DECIMAL(16, 10)) AS TZZH_ZXJZ_SR," ||
                "         CAST((CASE" ||
                "                WHEN TZZH_FE = 0 THEN" ||
                "                 1" ||
                "                WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "                 ROUND(IF((ZZC-DRYK-ZFZ) = 0, 0, (ZZC-ZFZ) / (ZZC-ZFZ-DRYK)), 10)" ||
                "                WHEN ZZC_SR > 0 AND ZZC = 0 THEN " ||
                "                 1" ||
                "                ELSE" ||
                "                 TZZH_LJJZ" ||
                "              END) AS DECIMAL(16, 8)) AS TZZH_LJJZ," ||
                "         (CASE" ||
                "           WHEN ZZC_SR = 0 AND ZZC > 0 THEN" ||
                "            1.0" ||
                "           ELSE" ||
                "            TZZH_LJJZ_SR" ||
                "         END) AS TZZH_LJJZ_SR," ||
                "         ZQSZ_JRCP," ||
                "         LXSR," ||
                "         ZCZJ_TOTC," ||
                "         ZRZJ_FOTC," ||
                "         LXZC_GPZY," ||
                "         JYFY_GPZY," ||
                "         LXZC_QT," ||
                "         QTSR,   " ||
                "         QTZC_FY," ||
                "         JYL," ||
                I_RQ || " as rq" ||
                "    FROM (SELECT NVL(TZZH_ID, 0) AS TZZH_ID," ||
                "                 T.KHH," ||
                "                 CAST(NVL(ZZC_SR, 0) AS DECIMAL(16, 2)) AS ZZC_SR," ||
                "                 CAST(NVL(ZZC, 0) AS DECIMAL(16, 2)) AS ZZC," ||
                "                 CAST(NVL(ZQSZ, 0) AS DECIMAL(16, 2)) AS ZQSZ," ||
                "                 CAST(NVL(ZQSZ_flt, 0) AS DECIMAL(16, 2)) AS ZQSZ_flt," ||
                "                 CAST(NVL(ZQSZ_dyp, 0) AS DECIMAL(16, 2)) AS ZQSZ_dyp," ||
                "                 CAST(NVL(ZJYE, 0) AS DECIMAL(16, 2)) AS ZJYE," ||
                "                 CAST(NVL(ZTZC, 0) AS DECIMAL(16, 2)) AS ZTZC," ||
                "                 CAST(NVL(QTZC, 0) AS DECIMAL(16, 2)) AS QTZC," ||
                "                 CAST(NVL(CRJE, 0) AS DECIMAL(16, 2)) AS CRJE," ||
                "                 CAST(NVL(QCJE, 0) AS DECIMAL(16, 2)) AS QCJE," ||
                "                 CAST(NVL(ZRSZ, 0) AS DECIMAL(16, 2)) AS ZRSZ," ||
                "                 CAST(NVL(ZCSZ, 0) AS DECIMAL(16, 2)) AS ZCSZ," ||
                "                 CAST(NVL(ZFZ_SR, 0) AS DECIMAL(16, 2)) AS ZFZ_SR," ||
                "                 CAST(NVL(ZFZ, 0) AS DECIMAL(16, 2)) AS ZFZ," ||
                "                 CAST(NVL(CCCB, 0) AS DECIMAL(16, 2)) AS CCCB," ||
                "                 CAST(NVL((ZZC - ZFZ) - (ZZC_SR - ZFZ_SR) - " ||
                "                                 (CRJE + ZRZJ_FOTC - QCJE - ZCZJ_TOTC) -" ||
                "                                 (ZRSZ - ZCSZ)," ||
                "                                 0) AS DECIMAL(16, 2)) AS DRYK," ||
                "                 CAST(ROUND(NVL(TZZH_FE, 0), 2) AS DECIMAL(16, 2)) AS TZZH_FE," ||
                "                 CAST(NVL(TZZH_FE_SR, 0) AS DECIMAL(16, 2)) AS TZZH_FE_SR," ||
                "                 CAST(NVL(TZZH_ZXFE, 0) AS DECIMAL(16, 2)) AS TZZH_ZXFE," ||
                "                 CAST(NVL(TZZH_ZXFE_SR, 0) AS DECIMAL(16, 2)) AS TZZH_ZXFE_SR," ||
                "                 CAST(IF(NVL(TZZH_ZXFE, 0) = 0," ||
                "                         1.0," ||
                "                         ROUND((NVL(ZZC, 0)-NVL(ZFZ,0)) / TZZH_ZXFE, 10)) AS" ||
                "                      DECIMAL(16, 10)) AS TZZH_ZXJZ," ||
                "                 CAST(ROUND(NVL(TZZH_ZXJZ_SR, 0),10) AS DECIMAL(16, 10)) AS TZZH_ZXJZ_SR," ||
                "                 CAST(IF(NVL(TZZH_FE, 0) = 0," ||
                "                         1.0," ||
                "                         ROUND((NVL(ZZC, 0)-NVL(ZFZ,0)) / TZZH_FE, 8)) AS" ||
                "                      DECIMAL(16, 8)) AS TZZH_LJJZ," ||
                "                 CAST(NVL(TZZH_LJJZ_SR, 0) AS DECIMAL(16, 8)) AS TZZH_LJJZ_SR," ||
                "                 CAST(NVL(ZQSZ_JRCP, 0) AS DECIMAL(16, 2)) AS ZQSZ_JRCP," ||
                "                 CAST(NVL(LXSR, 0) AS DECIMAL(16, 2)) AS LXSR," ||
                "                 CAST(NVL(ZCZJ_TOTC, 0) AS DECIMAL(16, 2)) AS ZCZJ_TOTC," ||
                "                 CAST(NVL(ZRZJ_FOTC, 0) AS DECIMAL(16, 2)) AS ZRZJ_FOTC," ||
                "                 CAST(NVL(LXZC_GPZY, 0) AS DECIMAL(16, 2)) AS LXZC_GPZY," ||
                "                 CAST(NVL(JYFY_GPZY, 0) AS DECIMAL(16, 2)) AS JYFY_GPZY," ||
                "                 CAST(NVL(LXZC_QT, 0) AS DECIMAL(16, 2)) AS LXZC_QT," ||
                "                 CAST(NVL(QTSR, 0) AS DECIMAL(16, 2)) AS QTSR," ||
                "                 CAST(NVL(QTZC_FY, 0) AS DECIMAL(16, 2)) AS QTZC_FY," ||
                "                 CAST(NVL(JYL, 0) AS DECIMAL(16, 2)) AS JYL" ||
                "            FROM (SELECT TZZH_ID," ||
                "                        S.KHH," ||
                "                        NVL(SUM(ZZC_SR), 0) AS ZZC_SR," ||
                "                        NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                        NVL(SUM(ZQSZ_flt), 0) + NVL(SUM(ZQSZ_dyp), 0) +" ||
                "                        NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) " ||
                "                         AS ZZC," ||
                "                        NVL(SUM(ZQSZ), 0) AS ZQSZ," ||
                "                        NVL(SUM(ZQSZ_flt), 0) AS ZQSZ_flt," ||
                "                        NVL(SUM(ZQSZ_dyp), 0) AS ZQSZ_dyp," ||
                "                        NVL(SUM(ZJYE), 0) AS ZJYE," ||
                "                        NVL(SUM(ZTZC), 0) AS ZTZC," ||
                "                        NVL(SUM(QTZC), 0) AS QTZC," ||
                "                        NVL(SUM(CRJE), 0) AS CRJE," ||
                "                        NVL(SUM(QCJE), 0) AS QCJE," ||
                "                        NVL(SUM(ZRSZ), 0) AS ZRSZ," ||
                "                        NVL(SUM(ZCSZ), 0) AS ZCSZ," ||
                "                        NVL(SUM(ZFZ_SR), 0) AS ZFZ_SR," ||
                "                        NVL(SUM(ZFZ), 0) AS ZFZ," ||
                "                        NVL(SUM(CCCB), 0) AS CCCB," ||
                "                        CASE" ||
                --第一天时无上日数据
                "                          WHEN NVL(SUM(TZZH_LJJZ_SR), 1.0) = 0 THEN" ||
               -- //如果上日累计净值为0，取本日资产作为份额
                "                           NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_FLT), 0) + NVL(SUM(ZQSZ_DYP), 0) +" ||
                "                           NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                           NVL(SUM(ZFZ), 0)" ||
                "                          ELSE" ||
                --第一天时无上日数据
                "                           IF(SUM(NVL(TZZH_FE_SR,0)) = 0," ||
                "                              NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                              NVL(SUM(ZQSZ_dyp), 0) + NVL(SUM(ZQSZ_flt), 0) +" ||
                "                              NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                              NVL(SUM(ZFZ), 0)," ||
                "                              NVL(SUM(TZZH_FE_SR), 0) +" ||
                "                              (NVL(SUM(CRJE), 0) + NVL(SUM(LXSR), 0) +" ||
                "                               NVL(SUM(ZRZJ_FOTC), 0) -" ||
                "                               NVL(SUM(QCJE), 0) -" ||
                "                               NVL(SUM(ZCZJ_TOTC), 0) -" ||
                "                               NVL(SUM(LXZC_GPZY), 0) -" ||
                "                               NVL(SUM(JYFY_GPZY), 0) -" ||
                "                               NVL(SUM(LXZC_QT), 0) + NVL(SUM(ZRSZ), 0) +" ||
                "                               NVL(SUM(QTSR), 0) - NVL(SUM(QTZC_FY), 0) -" ||
                "                               (NVL(SUM(ZCSZ), 0) - NVL(SUM(ZCSZ_YK), 0)) + NVL(SUM(HLXZ), 0)) /" ||
                "                              NVL(SUM(TZZH_LJJZ_SR), 1.0))" ||
                "                        END AS TZZH_FE," ||
                "                        NVL(SUM(TZZH_FE_SR), 0) AS TZZH_FE_SR," ||
                "                        NVL(SUM(TZZH_LJJZ_SR), 0) AS TZZH_LJJZ_SR," ||
                "                        CASE" ||
                "                          WHEN NVL(SUM(TZZH_ZXJZ_SR), 1.0) = 0 THEN" ||
               -- //如果上日累计净值为0，取本日资产作为最新份额
                "                           NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_dyp), 0) + NVL(SUM(ZQSZ_flt), 0) +" ||
                "                           NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                           NVL(SUM(ZFZ), 0)" ||
                "                          ELSE" ||
                "                           IF(SUM(NVL(TZZH_ZXFE_SR, 0)) = 0," ||
                "                              NVL(SUM(ZQSZ), 0) + NVL(SUM(ZJYE), 0) +" ||
                "                           NVL(SUM(ZQSZ_flt), 0) + NVL(SUM(ZQSZ_dyp), 0) +" ||
                "                              NVL(SUM(ZTZC), 0) + NVL(SUM(QTZC), 0) -" ||
                "                              NVL(SUM(ZFZ), 0)," ||
                "                              NVL(SUM(TZZH_ZXFE_SR), 0) +" ||
                "                              (NVL(SUM(CRJE), 0) + NVL(SUM(LXSR), 0) +" ||
                "                               NVL(SUM(ZRZJ_FOTC), 0) -" ||
                "                               NVL(SUM(QCJE), 0) -" ||
                "                               NVL(SUM(ZCZJ_TOTC), 0) -" ||
                "                               NVL(SUM(LXZC_GPZY), 0) -" ||
                "                               NVL(SUM(JYFY_GPZY), 0) -" ||
                "                               NVL(SUM(LXZC_QT), 0) + NVL(SUM(ZRSZ), 0) +" ||
                "                               NVL(SUM(QTSR), 0) - NVL(SUM(QTZC_FY), 0) -" ||
                "                               (NVL(SUM(ZCSZ), 0) - NVL(SUM(ZCSZ_YK), 0)) + NVL(SUM(HLXZ), 0)) /" ||
                "                              NVL(SUM(TZZH_ZXJZ_SR), 1.0))" ||
                "                        END AS TZZH_ZXFE," ||
                "                        NVL(SUM(TZZH_ZXFE_SR), 0) AS TZZH_ZXFE_SR," ||
                "                        NVL(SUM(NVL(TZZH_ZXJZ_SR,0)), 0) AS TZZH_ZXJZ_SR," ||
                "                        NVL(SUM(ZQSZ_JRCP), 0) AS ZQSZ_JRCP," ||
                "                        NVL(SUM(LXSR), 0) AS LXSR," ||
                "                        NVL(SUM(ZCZJ_TOTC), 0) AS ZCZJ_TOTC," ||
                "                        NVL(SUM(ZRZJ_FOTC), 0) AS ZRZJ_FOTC," ||
                "                        NVL(SUM(LXZC_GPZY), 0) AS LXZC_GPZY," ||
                "                        NVL(SUM(JYFY_GPZY), 0) AS JYFY_GPZY," ||
                "                        NVL(SUM(LXZC_QT), 0) AS LXZC_QT," ||
                "                        NVL(SUM(QTSR), 0) AS QTSR," ||
                "                        NVL(SUM(QTZC_FY), 0) AS QTZC_FY," ||
                "                        NVL(SUM(JYL), 0) AS JYL" ||
                "                   FROM " || F_IDS_GET_TABLENAME('sparkStatDR', I_KHH) || " s " ||
                "                  GROUP BY TZZH_ID, s.KHH) t left join cust.t_khxx_jjyw k on (t.khh=k.khh)" ||
                "                  WHERE NOT (K.XHRQ IS NOT NULL AND K.XHRQ<" || I_RQ || ")" ||
                ") T";


    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;


  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_stat_zc_r", I_RQ, I_KHH);
  END;
end;

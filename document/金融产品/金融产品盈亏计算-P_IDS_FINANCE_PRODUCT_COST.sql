create or replace procedure cust.p_ids_finance_product_cost(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_FINANCE_PRODUCT_COST
  *项目名称：IDS计算
  *文件说明：集中交易-金融产品成本盈亏计算

  创建人：胡阳明
  功能说明：集中交易-金融产品成本盈亏计算

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/6/20       创建
  燕居庆        v1.0.1        2019/8/19       金融产品份额改为柜台获取finance_sparkFPcpfe
  燕居庆        v1.0.2        2019/9/16       对标java-ids的4858版本：
  燕居庆        v1.0.3        2019/9/25       对标java-ids的5051版本：
                                                1.生成当日买入、当日卖出相关数据 卖出增加198类别
 燕居庆         v1.0.4        2019/12/17      增加ywdm=151 基金终止算卖出清仓处理
 燕居庆         v1.0.5        2019/12/18      增加 ywdm = 034’035 非交易过户处理
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING;
l_sqlWhereLastDay STRING;
l_lastDay INT;
BEGIN
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;


  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
  ELSE
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
  END IF;

  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPCbjsPre1', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPCbjsPre1', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPCbjsPre1', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPCbjsPre1', I_KHH);
    l_sqlBuf := 'select * from CUST.T_FP_CPFE_CBJS D where rq = ' || l_sqlWhereLastDay;--T-1

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;

  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPcpfe', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPcpfe', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPcpfe', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfe', I_KHH);
    l_sqlBuf := " SELECT T.KHH,T.JRCPZH,T.JRJGDM,T.CPDM,CPJC,CAST('1' AS STRING) SFFS,SUM(T.CPSL) AS CPSL,T.BZ,SUM(T.ZXSZ) AS ZXSZ," ||
                "   NULL AS KCRQ,NULL AS CCCB,NULL AS LJYK, NULL AS TBCCCB,NULL AS TBCBJ," ||
                "   NULL AS DRYK,NULL AS CBJ,T.APP_ID,T.CPFL,RQ" ||
                " FROM DSC_BAS.T_FP_CPFE_HIS T WHERE T.CPSL > 0 AND RQ = " || l_sqlWhereCurrentDay ||
                " GROUP BY T.KHH,T.APP_ID,T.JRJGDM,T.CPDM,T.BZ,T.CPFL,T.JRCPZH,CPJC,RQ";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  BEGIN
    /**
     * 已清仓现金宝
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfeXJB', I_KHH);
    l_sqlBuf := " SELECT T.KHH,T.JRCPZH,T.JRJGDM,T.CPDM,CPJC,CAST('1' AS STRING) SFFS,SUM(T.CPSL) AS CPSL,T.BZ,SUM(T.ZXSZ) AS ZXSZ," ||
                "   NULL AS CCCB,NULL AS LJYK, NULL AS TBCCCB,NULL AS TBCBJ," ||
                "   NULL AS DRYK,NULL AS CBJ,T.APP_ID,T.CPFL,RQ, KCRQ" ||
                " FROM DSC_BAS.T_FP_CPFE_HIS T WHERE T.CPDM = 'A72001' AND T.CPSL = 0 AND RQ = " || l_sqlWhereCurrentDay ||
                " GROUP BY T.KHH,T.APP_ID,T.JRJGDM,T.CPDM,T.BZ,T.CPFL,T.JRCPZH,CPJC,RQ,KCRQ";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPjgmxls', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPjgmxls', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPjgmxls', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPjgmxls', I_KHH);
    l_sqlBuf := 'select * from cust.T_FP_JGMXLS_HIS where qrrq = ' || l_sqlWhereCurrentDay;

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPcpdm', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPcpdm', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPcpdm', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpdm', I_KHH);
    l_sqlBuf := 'select * from cust.T_JRCPDM ';

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
  --------------------------获取金融产品份额--------------------------------
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPcpfePre1', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPcpfePre1', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPcpfePre1', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfePre1', I_KHH);
    l_sqlBuf := 'select * from CUST.T_FP_CPFE_HIS D where rq = ' || l_sqlWhereLastDay;--T-1

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  BEGIN
    /**
    -------------------------获取金融产品行情--------------------------
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkFPhq', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkFPhq', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkFPhq', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkFPhq', I_KHH);
    l_sqlBuf := 'select * from DSC_BAS.T_JRCPHQ_HIS where rq = ' || I_RQ;

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

-------------------------------------   //1、生成当日买入、当日卖出相关数据---------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_1', I_KHH);
    l_sqlBuf :=
                "SELECT A.TZZH_ID," || I_RQ || " AS RQ," ||
                "       A.KHH," ||
                "       A.APP_ID," ||
                "       A.JRJGDM," ||
                "       A.JRCPZH," ||
                "       A.CPDM," ||
                "       A.BZ," ||
                "       CAST(A.YSSL AS DECIMAL(22, 2)) AS DRYSSL," ||
                "       CAST(A.MRJE AS DECIMAL(16, 2)) AS DRMRJE," ||
                "       CAST(A.MRSL AS DECIMAL(22, 2)) AS DRMRSL," ||
                "       CAST(A.MCJE AS DECIMAL(16, 2)) AS DRMCJE," ||
                "       CAST(A.MCSL AS DECIMAL(22, 2)) AS DRMCSL," ||
                "       CAST(A.JYFY AS DECIMAL(16, 2)) AS DRJYFY" ||
                "  FROM (SELECT 0 AS TZZH_ID," ||
                "               APP_ID," ||
                "               KHH," ||
                "               JRCPZH," ||
                "               JRJGDM," ||
                "               CPDM," ||
                "               BZ," ||
                --122：申购；123：预约申购;139:定时定额投资；130：认购结果；143：红利发放；144：强行调增；034：非交易过户转入
                "               SUM(CASE WHEN YWDM IN ('122', '123', '139', '130', '143', '144', '034') THEN ABS(QRFE)" ||
                --124:赎回；125：预约赎回；126：转销售人/机构;142:强行赎回；143：红利发放；145:强行调减;150:基金清盘；151：基金终止
                "                        WHEN YWDM IN ('124', '125','126', '142', '143', '145', '150', '151') THEN -ABS(QRFE)" ||
                "                        ELSE 0 " ||
                "                   END) AS YSSL," ||
                "               SUM(CASE" ||
                "                       WHEN YWDM IN ('122', '123', '139', '130', '144','034') THEN" ||
                "                        ABS(QRJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                   END) AS MRJE," ||
                "               SUM(CASE" ||
                "                       WHEN YWDM IN ('122', '123', '139', '130', '143', '144','034') THEN" ||
                "                        ABS(QRFE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                   END) AS MRSL," ||
                "               SUM(CASE" ||
                "                       WHEN YWDM IN ('124', '125', '142', '143', '145', '150', '198','126', '151', '035') THEN" ||
                "                        ABS(QRJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                   END) AS MCJE," ||
                "               SUM(CASE" ||
                "                       WHEN YWDM IN ('124', '125', '142', '145', '150', '198','126', '151', '035') THEN" ||
                "                        ABS(QRFE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                   END) AS MCSL," ||
                "               SUM(SXF) AS JYFY" ||
                "          FROM " || F_IDS_GET_TABLENAME('finance_sparkFPjgmxls', I_KHH) ||
                "         WHERE YWDM IN ('122', '123', '124', '125','126' ,'139', '142', '130', '143', '144', '145', '150', '198', '151','034', '035')" ||
                "         GROUP BY KHH, JRCPZH, JRJGDM, CPDM, BZ,APP_ID) A";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
-------------------- //2、将本日数据和上日累计数据进行累计处理------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_2', I_KHH);
    l_sqlBuf :=
                "SELECT 0 AS TZZH_ID," ||
                "       D.KHH," ||
                "       D.JRJGDM," ||
                "       D.JRCPZH," ||
                "       D.CPDM," ||
                "       D.BZ," ||
                "       D.APP_ID," ||
                "       nvl(SUM(DRYSSL), 0) DRYSSL," ||
                "       nvl(SUM(DRMRJE), 0) DRMRJE," ||
                "       nvl(SUM(DRMRSL), 0) DRMRSL," ||
                "       nvl(SUM(DRMCJE), 0) DRMCJE," ||
                "       nvl(SUM(DRMCSL), 0) DRMCSL," ||
                "       nvl(SUM(DRJYFY), 0) DRJYFY," ||
                "       nvl(SUM(LJMRJE), 0) LJMRJE," ||
                "       nvl(SUM(LJMRSL), 0) LJMRSL," ||
                "       nvl(SUM(LJMCJE), 0) LJMCJE," ||
                "       nvl(SUM(LJMCSL), 0) LJMCSL," ||
                "       nvl(SUM(LJPGSL), 0) LJPGSL," ||
                "       nvl(SUM(LJJYFY), 0) LJJYFY" ||
                "  FROM (SELECT KHH," ||
                "               JRJGDM," ||
                "               JRCPZH," ||
                "               CPDM," ||
                "               BZ," ||
                "               APP_ID," ||
                "               SUM(DRYSSL) AS DRYSSL," ||
                "               SUM(DRMRJE) AS DRMRJE," ||
                "               SUM(DRMRSL) AS DRMRSL," ||
                "               SUM(DRMCJE) AS DRMCJE," ||
                "               SUM(DRMCSL) AS DRMCSL," ||
                "               SUM(DRJYFY) AS DRJYFY," ||
                "               SUM(DRMRJE) AS LJMRJE," ||
                "               SUM(DRMRSL) AS LJMRSL," ||
                "               SUM(DRMCJE) AS LJMCJE," ||
                "               SUM(DRMCSL) AS LJMCSL," ||
                "               0 AS LJPGSL," ||
                "               SUM(DRJYFY) AS LJJYFY" ||
                "          FROM " || F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_1', I_KHH) || "  " ||
                "         GROUP BY KHH, APP_ID,JRJGDM, JRCPZH, CPDM, BZ" ||
                "        UNION ALL" ||
                "        SELECT KHH," ||
                "               JRJGDM," ||
                "               JRCPZH," ||
                "               CPDM," ||
                "               BZ," ||
                "               APP_ID," ||
                "               0 AS DRYSSL," ||
                "               0 AS DRMRJE," ||
                "               0 AS DRMRSL," ||
                "               0 AS DRMCJE," ||
                "               0 AS DRMCSL," ||
                "               0 AS DRJYFY," ||
                "               0 AS LJMRJE," ||
                "               0 AS LJMRSL," ||
                "               0 AS LJMCJE," ||
                "               0 AS LJMCSL," ||
                "               0 AS LJPGSL," ||
                "               0 AS LJJYFY " ||
                "          FROM " || F_IDS_GET_TABLENAME('finance_sparkFPcpfeXJB', I_KHH) || --记录现金宝交易成本轨迹
                "        UNION ALL" ||
                "        SELECT KHH," ||
                "               JRJGDM," ||
                "               JRCPZH," ||
                "               CPDM," ||
                "               BZ," ||
                "               APP_ID," ||
                "               0 AS DRYSSL," ||
                "               0 AS DRMRJE," ||
                "               0 AS DRMRSL," ||
                "               0 AS DRMCJE," ||
                "               0 AS DRMCSL," ||
                "               0 AS DRJYFY," ||
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJMRJE, 0)) LJMRJE," ||  -- 上一日清仓的累计值就不能累计到当日
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJMRSL, 0)) LJMRSL," ||  -- 上一日清仓的累计值就不能累计到当日
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJMCJE, 0)) LJMCJE," ||  -- 上一日清仓的累计值就不能累计到当日
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJMCSL, 0)) LJMCSL," ||  -- 上一日清仓的累计值就不能累计到当日
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJPGSL, 0)) LJPGSL," ||  -- 上一日清仓的累计值就不能累计到当日
                "               IF(nvl(CPSL, 0) = 0, 0, nvl(LJJYFY, 0)) LJJYFY " ||  -- 上一日清仓的累计值就不能累计到当日
                "          FROM " || F_IDS_GET_TABLENAME('finance_sparkFPCbjsPre1', I_KHH) || "  ) D" ||
                " GROUP BY D.KHH, D.JRJGDM,D.APP_ID, D.JRCPZH, D.CPDM, D.BZ";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

 -----------------------------------    //3、获取当日的份额市值以及上日的成本盈亏等数据-----------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_3', I_KHH);
    l_sqlBuf :=
                "SELECT A.TZZH_ID," ||
                  I_RQ || " AS RQ," ||
                "       A.KHH," ||
                "       A.JRJGDM," ||
                "       A.JRCPZH," ||
                "       A.CPDM," ||
                "       A.BZ," ||
                "       A.APP_ID," ||
                "       nvl(B.CPSL, 0) AS CPSL," ||
                "       nvl(B.ZXSZ, 0) AS ZQSZ," ||
                "       nvl(DRYSSL, 0) AS DRYSSL," ||
                "       nvl(DRMRJE, 0) AS DRMRJE," ||
                "       nvl(DRMRSL, 0) AS DRMRSL," ||
                "       nvl(DRMCJE, 0) AS DRMCJE," ||
                "       nvl(DRMCSL, 0) AS DRMCSL," ||
                "       nvl(DRJYFY, 0) AS DRJYFY," ||
                "       CASE" ||
                "           WHEN nvl(C.CPSL, 0) = 0 AND nvl(DRMRSL, 0) = nvl(DRMCSL, 0) THEN" ||
                "            nvl(DRMRJE, 0)" ||
                "           ELSE" ||
                "            nvl(LJMRJE, 0)" ||
                "       END AS LJMRJE," ||  -- 考虑当日买入卖出清仓情况处理
                "       CASE" ||
                "           WHEN nvl(C.CPSL, 0) = 0 AND nvl(DRMRSL, 0) = nvl(DRMCSL, 0) THEN" ||
                "            nvl(DRMRSL, 0)" ||
                "           ELSE" ||
                "            nvl(LJMRSL, 0)" ||
                "       END AS LJMRSL," ||  -- 考虑当日买入卖出清仓情况处理
                "       CASE" ||
                "           WHEN nvl(C.CPSL, 0) = 0 AND nvl(DRMRSL, 0) = nvl(DRMCSL, 0) THEN" ||
                "            nvl(DRMCJE, 0)" ||
                "           ELSE" ||
                "            nvl(LJMCJE, 0)" ||
                "       END AS LJMCJE," ||  -- 考虑当日买入卖出清仓情况处理
                "       CASE" ||
                "           WHEN nvl(C.CPSL, 0) = 0 AND nvl(DRMRSL, 0) = nvl(DRMCSL, 0) THEN" ||
                "            nvl(DRMCSL, 0)" ||
                "           ELSE" ||
                "            nvl(LJMCSL, 0)" ||
                "       END AS LJMCSL," ||  -- 考虑当日买入卖出清仓情况处理
                "       CASE" ||
                "           WHEN nvl(C.CPSL, 0) = 0 AND nvl(DRMRSL, 0) = nvl(DRMCSL, 0) THEN" ||
                "            nvl(DRJYFY, 0)" ||
                "           ELSE" ||
                "            nvl(LJJYFY, 0)" ||
                "       END AS LJJYFY," ||  -- 考虑当日买入卖出清仓情况处理
                "       nvl(C.CCCB, 0) SRCCCB," ||
                "       nvl(C.LJYK, 0) SRLJYK," ||
                "       nvl(C.CPSL, 0) SRCPSL," ||
                "       nvl(C.ZXSZ, 0) SRZQSZ," ||
                "       ROUND(IF(nvl(C.CPSL, 0) > 0, nvl(C.CCCB, 0) / nvl(C.CPSL, 0), 0), 4) AS SRCBJ" ||
                "  FROM " || F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_2', I_KHH) || "  A" ||
                "  LEFT JOIN (SELECT JRJGDM, CPDM, JRCPZH, CPSL, ZXSZ, KHH, APP_ID" ||
                "               FROM " || F_IDS_GET_TABLENAME('finance_sparkFPcpfe', I_KHH) || ") B" ||
                "    ON (A.JRJGDM = B.JRJGDM" ||
                "   AND A.CPDM = B.CPDM" ||
                "   AND A.JRCPZH = B.JRCPZH" ||
                "   AND A.KHH = B.KHH" ||
                "   AND A.APP_ID = B.APP_ID)" ||
                "  LEFT JOIN (SELECT JRJGDM, CPDM, JRCPZH, CPSL, ZXSZ, CCCB, LJYK, KHH, APP_ID" ||
                "               FROM " || F_IDS_GET_TABLENAME('finance_sparkFPcpfePre1', I_KHH) || ") C" ||
                "    ON A.JRJGDM = C.JRJGDM" ||
                "   AND A.CPDM = C.CPDM" ||
                "   AND A.JRCPZH = C.JRCPZH" ||
                "   AND A.KHH = C.KHH" ||
                "   AND A.APP_ID = C.APP_ID"    ;

      --put_line(l_sqlBuf)     ;
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;


 ----------------------------------  //4、生成最终数据------------------------------------------
   BEGIN
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPCbjsResult', I_KHH);
    l_sqlBuf :=
                "SELECT T1.TZZH_ID," ||
                "           T1.KHH," ||
                "           T1.JRCPZH," ||
                "           T1.JRJGDM," ||
                "           T1.CPDM," ||
                "           T1.CPFL," ||
                "           T1.BZ," ||
                "           CASE" ||
                "               WHEN (T1.KCRQ IS NULL OR T1.KCRQ = 0) AND T2.KCRQ IS NOT NULL THEN" ||
                "                T2.KCRQ" ||
                "               ELSE" ||
                "                T1.KCRQ" ||
                "           END AS KCRQ," ||
                "           T1.CPSL," ||
                "           CAST(T1.ZQSZ AS DECIMAL(16,2)) AS ZQSZ," ||
                "           CAST(T1.DRYSSL AS DECIMAL(22,2)) AS DRYSSL," ||
                "           CAST(T1.DRMRJE AS DECIMAL(16,2)) AS DRMRJE," ||
                "           CAST(T1.DRMRSL AS DECIMAL(22,2)) AS DRMRSL," ||
                "           0 AS DRPGJE," ||
                "           0 AS DRPGSL," ||
                "           CAST(T1.DRMCJE AS DECIMAL(16,2)) AS DRMCJE," ||
                "           CAST(T1.DRMCSL AS DECIMAL(22,2)) AS DRMCSL," ||
                "           CAST(T1.DRJYFY AS DECIMAL(16,2)) AS DRJYFY," ||
                "           0 AS SRCCJJ," ||
                "           CAST(T1.LJMRJE AS DECIMAL(16,2)) AS LJMRJE," ||
                "           CAST(T1.LJMRSL AS DECIMAL(22,2)) AS LJMRSL," ||
                "           0 AS LJPGJE," ||
                "           0 AS LJPGSL," ||
                "           CAST(T1.LJMCJE AS DECIMAL(16,2)) AS LJMCJE," ||
                "           CAST(T1.LJMCSL AS DECIMAL(22,2)) AS LJMCSL," ||
                "           CAST(T1.LJJYFY AS DECIMAL(16,2)) AS LJJYFY," ||
                "           T1.SRCPSL," ||
                "           CAST(T1.SRZQSZ AS DECIMAL(16,2)) AS SRZQSZ," ||
                "           CAST(T1.SRCBJ AS DECIMAL(9,4)) AS SRCBJ," ||
                "           CAST(T1.SRCCCB AS DECIMAL(16,2)) AS SRCCCB," ||
                "           CAST(T1.SRLJYK AS DECIMAL(16,2)) AS SRLJYK," ||
                "           CAST(T1.CCCB AS DECIMAL(16,2)) AS CCCB," ||
                "           CAST(T1.LJYK AS DECIMAL(16,2)) AS LJYK," ||
                "           T1.APP_ID," ||
                "           T1.RQ" ||
                "      FROM (SELECT T.TZZH_ID," ||
                "                   T.RQ," ||
                "                   T.KHH," ||
                "                   T.JRJGDM," ||
                "                   T.JRCPZH," ||
                "                   T.CPDM," ||
                "                   T.BZ," ||
                "                   T.APP_ID," ||
                "                   T.CPSL," ||
                "                   T.ZQSZ," ||
                "                   T.DRYSSL," ||
                "                   T.DRMRJE," ||
                "                   T.DRMRSL," ||
                "                   T.DRMCJE," ||
                "                   T.DRMCSL," ||
                "                   T.DRJYFY," ||
                "                   T.LJMRJE," ||
                "                   T.LJMRSL," ||
                "                   T.LJMCJE," ||
                "                   T.LJMCSL," ||
                "                   T.LJJYFY," ||
                "                   T.SRCCCB," ||
                "                   T.SRLJYK," ||
                "                   T.SRCPSL," ||
                "                   T.SRZQSZ," ||
                "                   T.SRCBJ," ||
                "                   nvl(D.CPFL, '0') AS CPFL," ||
                "                   ROUND(CASE" ||
                "                             WHEN DRMRSL > DRMCSL THEN" ||
                "                              cast(SRCCCB + DRMRJE - DRMCJE AS decimal(16,2))" ||
                "                             WHEN DRMRSL < DRMCSL THEN" ||
                --不应该是srcccb-srcbj*drmcsl+drmrje么？
                "                              cast(SRCCCB - SRCBJ * (DRMCSL - DRMRSL) AS decimal(16,2))" ||
                "                             ELSE" ||
                "                              SRCCCB" ||
                "                         END," ||
                "                         2) AS CCCB," ||                          --持仓成本发生变化
                "                   ROUND(CASE" ||
                "                             WHEN DRMRSL > DRMCSL  THEN" ||
                "                              SRLJYK" ||
                "                             WHEN DRMRSL < DRMCSL  THEN" ||
                --为什么不是srljyk+(DRMCJE - DRMRJE)-srcbj*drmcsl  +srcbj*drmrsl
                "                              SRLJYK + (DRMCJE - DRMRJE) - SRCBJ * (DRMCSL - DRMRSL)" ||
                "                             ELSE" ||
                "                              SRLJYK + (DRMCJE - DRMRJE)" ||
                "                         END," ||
                "                         2) AS LJYK," ||
                "                   CASE" ||
                "                       WHEN (T.DRMRSL > 0 AND T.DRMRSL = T.LJMRSL AND T.CPSL > 0) OR" ||
                "                            (T.DRMRSL > 0 AND T.DRMRSL = T.LJMRSL AND T.DRMRSL = T.DRMCSL AND" ||
                "                            T.CPSL = 0) THEN" ||
                "                        T.RQ" ||
                "                       ELSE" ||
                "                        0" ||
                "                   END AS KCRQ" ||
                "              FROM  " || F_IDS_GET_TABLENAME('finance_sparkFPcpfeCbjs_3', I_KHH) || " T" ||
                "              LEFT JOIN " || F_IDS_GET_TABLENAME('finance_sparkFPcpdm', I_KHH) || " D" ||
                "                ON T.JRJGDM = D.JRJGDM" ||
                "               AND T.CPDM = D.CPDM" ||
                "               AND T.APP_ID = D.APP_ID" ||
                "       ) T1" ||
                "      LEFT JOIN (SELECT A.TZZH_ID, A.JRCPZH, A.JRJGDM, A.CPDM, A.KCRQ, A.KHH, A.APP_ID" ||
                "                   FROM " || F_IDS_GET_TABLENAME('finance_sparkFPCbjsPre1', I_KHH) || " A" ||
                "                  WHERE (A.CPSL > 0 OR a.cpdm = 'A72001')" ||
                "                    AND A.KCRQ <> 0) T2" ||
                "        ON T1.TZZH_ID = T2.TZZH_ID" ||
                "       AND T1.KHH = T2.KHH" ||
                "       AND T1.JRCPZH = T2.JRCPZH" ||
                "       AND T1.JRJGDM = T2.JRJGDM" ||
                "       AND T1.CPDM = T2.CPDM" ||
                "       AND T1.APP_ID = T2.APP_ID";

              -- put_line(l_sqlBuf);
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

 ----------------------------------- //5、更新到表--------------------------------------
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_FP_CPFE_CBJS", I_RQ, I_KHH);
  END;

----------------------------回写T_FP_CPFE_HIS------------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('finance_sparkFPcpfeResult', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_finance_sparkFPcpfeResult', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_finance_sparkFPcpfeResult', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('finance_sparkFPcpfeResult', I_KHH);
    l_sqlBuf :=
                "SELECT T.KHH," ||
                "       T.JRCPZH," ||
                "       T.JRJGDM," ||
                "       T.CPDM," ||
                "       T.CPJC," ||
                "       T.SFFS," ||
                "       T.CPSL," ||
                "       T.BZ," ||
                "       T.ZXSZ," ||
                "       T.CPFL," ||
                "       D.KCRQ AS KCRQ," ||
                "       NVL(D.CCCB, 0) AS CCCB," ||
                "       NVL(D.LJYK, 0) AS LJYK," ||
                "       NVL(D.TBCCCB, 0) AS TBCCCB," ||
                "       CASE" ||
                "           WHEN T.CPSL = 0 THEN" ||
                "            0" ||
                "           ELSE" ||
                "            NVL(D.TBCCCB, 0) / T.CPSL" ||
                "       END AS TBCBJ," ||
                "       NVL(D.DRYK, 0) AS DRYK," ||
                "       CASE" ||
                "           WHEN T.CPSL = 0 THEN" ||
                "            0" ||
                "           ELSE" ||
                "            NVL(D.CCCB, 0) / T.CPSL" ||
                "       END AS CBJ,T.APP_ID, rq" ||
                "  FROM (SELECT KHH," ||
                "               JRCPZH," ||
                "               JRJGDM," ||
                "               CPDM," ||
                "               CPJC," ||
                "               SFFS," ||
                "               CPSL," ||
                "               BZ," ||
                "               ZXSZ," ||
                "               CPFL," ||
                "               KCRQ," ||
                "               CCCB," ||
                "               LJYK," ||
                "               TBCCCB," ||
                "               TBCBJ," ||
                "               DRYK," ||
                "               CBJ," ||
                "               APP_ID," ||
                I_RQ || " as  rq" ||
                "          FROM  " || F_IDS_GET_TABLENAME('finance_sparkFPcpfe', I_KHH) ||
                "           UNION ALL " ||
                "SELECT KHH," ||
                "               JRCPZH," ||
                "               JRJGDM," ||
                "               CPDM," ||
                "               CPJC," ||
                "               SFFS," ||
                "               CPSL," ||
                "               BZ," ||
                "               ZXSZ," ||
                "               CPFL," ||
                "               KCRQ," ||
                "               CCCB," ||
                "               LJYK," ||
                "               TBCCCB," ||
                "               TBCBJ," ||
                "               DRYK," ||
                "               CBJ," ||
                "               APP_ID," ||
                I_RQ || " as  rq" ||
                 "          FROM  " || F_IDS_GET_TABLENAME('finance_sparkFPcpfeXJB', I_KHH) || " ) T" ||
                "  LEFT JOIN (SELECT JRJGDM," ||
                "                    CPDM," ||
                "                    JRCPZH," ||
                "                    TZZH_ID," ||
                "                    KHH," ||
                "                    APP_ID," ||
                "                    CCCB," ||
                "                    LJYK," ||
                "                    CCCB - LJYK AS TBCCCB," ||
                "                    CPSL," ||
                "                    ZQSZ," ||
                "                    SRZQSZ," ||
                "                    DRMRJE," ||
                "                    DRMCJE," ||
                "                    KCRQ," ||
                "                    ZQSZ - SRZQSZ - (DRMRJE - DRMCJE) AS DRYK" ||
                "               FROM  " || F_IDS_GET_TABLENAME('finance_sparkFPCbjsResult', I_KHH) || " ) D" ||
                "    ON T.KHH = D.KHH" ||
                "   AND T.JRJGDM = D.JRJGDM" ||
                "   AND T.CPDM = D.CPDM" ||
                "   AND T.JRCPZH = D.JRCPZH" ||
                "   AND T.APP_ID = D.APP_ID";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN

    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_FP_CPFE_HIS", I_RQ, I_KHH);
  END;



----------------------------------------------------------------------------------------

  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkFPTzsyResult', I_KHH);
    l_sqlBuf :=
                " SELECT " ||
                "           D.KCRQ," ||
                "           D.TZZH_ID," ||
                "           D.KHH," ||
                "           D.JRJGDM," ||
                "           D.JRCPZH," ||
                "           D.CPDM," ||
                "           nvl(T.CPFL,'0') AS CPFL," ||
                "           D.BZ," ||
                "           D.LJMRJE AS MRJE," ||
                "           D.LJMCJE AS MCJE," ||
                "           D.LJJYFY AS JYFY," ||
                "           D.LJYK AS LJYK," ||
                "           CAST(D.ZQSZ - D.SRZQSZ - (D.DRMRJE - D.DRMCJE) AS DECIMAL(16, 2)) AS DRYK," ||
                "           D.APP_ID," ||
                           I_RQ || " as qcrq" ||
                "      FROM (SELECT T.RQ," ||
                "                   T.KHH," ||
                "                   T.TZZH_ID," ||
                "                   T.JRCPZH," ||
                "                   T.BZ," ||
                "                   T.APP_ID," ||
                "                   T.JRJGDM," ||
                "                   T.CPDM," ||
                "                   T.CPFL," ||
                "                   T.KCRQ," ||
                "                   T.CPSL," ||
                "                   T.DRMCSL," ||
                "                   T.LJMRJE," ||
                "                   T.LJMCJE," ||
                "                   T.LJJYFY," ||
                "                   T.CCCB," ||
                "                   T.LJYK," ||
                "                   T.ZQSZ," ||
                "                   T.SRCPSL," ||
                "                   T.SRZQSZ," ||
                "                   T.DRMRJE," ||
                "                   T.DRMCJE" ||
                "              FROM " || F_IDS_GET_TABLENAME('finance_sparkFPCbjsResult', I_KHH) || " T) D" ||
                "      LEFT JOIN " || F_IDS_GET_TABLENAME('finance_sparkFPcpdm', I_KHH) || " T" ||
                "        ON T.JRJGDM = D.JRJGDM" ||
                "       AND T.CPDM = D.CPDM" ||
                "       AND T.APP_ID = D.APP_ID" ||
                "     WHERE D.CPSL = 0" ||
                "     AND D.CPDM <> 'A72001'" || --去除现金宝
                "       AND D.DRMCSL > 0";

    -- put_line(l_sqlBuf);
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;


  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_FP_TZSY", I_RQ, I_KHH);
  END;

end;

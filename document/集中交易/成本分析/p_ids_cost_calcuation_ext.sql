create or replace procedure cust.p_ids_cost_calcuation_ext(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_COST_CALCUATION_EXT
  *项目名称：IDS计算
  *文件说明：集中交易-成本盈亏计算——扩展

  创建人：燕居庆
  功能说明：集中交易-成本盈亏计算——扩展

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/7/18       创建
  燕居庆        v1.0.1        2019/8/19       调整待交收数据获取为DSC_BAS层数据
  燕居庆        v1.0.2        2019/9/13       对标JAVA-IDS的 4858 版本,调整清仓待交收的交收日期判断settle_date_2
  燕居庆        v1.0.3        2019/9/25       对标JAVA-IDS 5051版本
                                                1.待交收投资损益增加RQ条件
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_sql STRING; --
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING;

l_sqlWhereLastDay STRING;
l_lastDay INT;
l_hlcsHKD DECIMAL(12,6);
l_hlcsUSD DECIMAL(12,6);
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
l_count INT;
BEGIN
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;

  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay || ' ';
  ELSE
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
  END IF;
  -- 临时表创建temp库下

  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;

  ELSE
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
  END IF;

  BEGIN
  -----------------------        //1、取出在统计日期最后一笔清算交割单，获取相应的持仓成本---------------
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkCbjsExt', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkCbjsExt', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkCbjsExt', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkCbjsExt', I_KHH);
    l_sqlBuf :=
                "SELECT D.TZZH_ID," ||
                "       D.KHH," ||
                "       D.GDH," ||
                "       D.JYS," ||
                "       D.ZQDM," ||
                "       D.SEQNO_QS," ||
                "       D.JYLB_QS," ||
                "       D.CCCB_QS" ||
                "  FROM (SELECT T.TZZH_ID," ||
                "               T.KHH," ||
                "               T.GDH," ||
                "               T.JYS," ||
                "               T.ZQDM," ||
                "               T.SEQNO_QS," ||
                "               T.JYLB_QS," ||
                "               T.CCCB_QS," ||
                "               T.LJYK_QS," ||
                "               ROW_NUMBER() OVER(PARTITION BY T.KHH, T.JYS, T.ZQDM ORDER BY KHH, GDH, JYS, ZQDM, SEQNO_QS DESC) RN" ||
                "          FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH) || " T" ||
                "         WHERE T.ZT_QS <> 0) D" ||
                " WHERE RN = 1";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;



 ----------------------        //2、更新成本计算中间表数据盈亏数据-------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkCbjsResultYK', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkCbjsResultYK', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkCbjsResultYK', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkCbjsResultYK', I_KHH);
    l_sqlBuf :=
                "SELECT " ||
                "         T.TZZH_ID," ||
                "         T.KHH," ||
                "         T.GDH," ||
                "         T.JYS," ||
                "         T.ZQDM," ||
                "         T.ZQLB," ||
                "         T.BZ," ||
                "         T.KCRQ," ||
                "         T.ZQSL," ||
                "         T.ZQSZ," ||
                "         T.DRYSSL," ||
                "         T.DRMRJE," ||
                "         T.DRMRSL," ||
                "         T.DRPGJE," ||
                "         T.DRPGSL," ||
                "         T.DRMCJE," ||
                "         T.DRMCSL," ||
                "         T.DRJYFY," ||
                "         T.SRCCJJ," ||
                "         T.LJMRJE," ||
                "         T.LJMRSL," ||
                "         T.LJPGJE," ||
                "         T.LJPGSL," ||
                "         T.LJMCJE," ||
                "         T.LJMCSL," ||
                "         T.LJJYFY," ||
                "         T.SRZQSL," ||
                "         T.SRZQSZ," ||
                "         T.SRCBJ," ||
                "         T.SRCCCB," ||
                "         T.SRLJYK," ||
                "         T.CCCB," ||
                "         T.LJYK," ||
                "         (CASE" ||
                "           WHEN D.KHH IS NOT NULL THEN" ||
                "            D.CCCB_QS" ||
                "           ELSE" ||
                "            T.CCCB_TZ" ||
                "         END) AS CCCB_TZ," ||
                "         CAST((CASE" ||
                "           WHEN D.KHH IS NOT NULL AND J.KHH IS NOT NULL THEN" ||
                "            CASE" ||
                "              WHEN T.SRZQSL = 0 THEN" ||
                "               J.SXYK_QS + (J.YK_ZR - J.YK_ZC)" ||
                "              ELSE" ||
                "               T.SRLJYK + J.SXYK_QS + (J.YK_ZR - J.YK_ZC)" ||
                "            END" ||
                "           ELSE" ||
                "            T.LJYK_TZ" ||
                "         END) AS DECIMAL(16,2)) AS LJYK_TZ," ||
                "         T.DRHBCFJE_ZR," ||
                "         T.DRHBCFSL_ZR," ||
                "         T.DRHBCFJE_ZC," ||
                "         T.DRHBCFSL_ZC," ||
                "         CAST((CASE" ||
                "           WHEN D.KHH IS NOT NULL AND J.KHH IS NOT NULL THEN" ||
                "            J.CCCB_ZR" ||
                "           ELSE" ||
                "            T.DRHBCFJE_ZR_TZ" ||
                "         END) AS DECIMAL(16,2)) AS DRHBCFJE_ZR_TZ," ||
                "         CAST((CASE" ||
                "           WHEN D.KHH IS NOT NULL AND J.KHH IS NOT NULL THEN" ||
                "            J.CCCB_ZC" ||
                "           ELSE" ||
                "            T.DRHBCFJE_ZC_TZ" ||
                "         END) AS DECIMAL(16,2)) AS DRHBCFJE_ZC_TZ," ||
                "         CAST((CASE" ||
                "           WHEN D.KHH IS NOT NULL AND J.KHH IS NOT NULL THEN" ||
                "            J.YK_ZR" ||
                "           ELSE" ||
                "            T.YK_ZR" ||
                "         END) AS DECIMAL(16,2)) AS YK_ZR," ||
                "         CAST((CASE" ||
                "           WHEN D.KHH IS NOT NULL AND J.KHH IS NOT NULL THEN" ||
                "            J.YK_ZC" ||
                "           ELSE" ||
                "            T.YK_ZC" ||
                "         END) AS DECIMAL(16,2)) AS YK_ZC," ||
                "         T.DRSHDQSL," ||
                "         T.DRSHDQJE," ||
                "         T.DRSHSYSL," ||
                "         T.SRSHSYSL," ||
                "         T.QCRQ," ||
                "         T.DRSHSL_ETFHB," ||
                "         T.DRSGSL_ETFHB," ||
                "         T.DRSHJE_SJ_ETFHB," ||
                "         T.DRSGJE_SJ_ETFHB," ||
                "         T.DRSHJE_SS_ETFHB," ||
                "         T.DRSGJE_SF_ETFHB," ||
                "         T.LJSHJE_SJ_ETFHB," ||
                "         T.LJSGJE_SJ_ETFHB," ||
                "         T.LJSHJE_SS_ETFHB," ||
                "         T.LJSGJE_SF_ETFHB" ||
                "    FROM " || F_IDS_GET_TABLENAME('sparkCbjsResult', I_KHH) ||" T" ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkCbjsExt', I_KHH) ||"  D" ||
                "      ON (T.TZZH_ID = D.TZZH_ID AND T.GDH = D.GDH AND T.JYS = D.JYS AND" ||
                "         T.ZQDM = D.ZQDM AND T.KHH = D.KHH)" ||
                "    LEFT JOIN (SELECT J.TZZH_ID," ||
                "                      J.KHH," ||
                "                      J.GDH," ||

                "                      J.JYS," ||
                "                      J.ZQDM," ||
                "                      F.JJLX," ||
                "                      NVL(SUM(J.SXYK_QS), 0) AS SXYK_QS," ||
                "                      NVL(SUM(J.YK_ZR), 0) AS YK_ZR," ||
                "                      NVL(SUM(J.YK_ZC), 0) AS YK_ZC," ||
                "                      NVL(SUM(J.CCCB_ZR), 0) AS CCCB_ZR," ||
                "                      NVL(SUM(J.CCCB_ZC), 0) AS CCCB_ZC" ||
                "                 FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH) ||" J" ||
                "                 LEFT JOIN cust.v_fjjjxx F" ||
                "                   ON (F.JYS = J.JYS AND J.ZQDM = F.JJDM)" ||
                "                GROUP BY J.TZZH_ID, J.KHH, J.GDH, J.JYS, J.ZQDM, F.JJLX) J" ||
                "      ON (T.TZZH_ID = J.TZZH_ID AND T.GDH = J.GDH AND T.JYS = J.JYS AND" ||
                "         T.ZQDM = J.ZQDM AND T.KHH = J.KHH)";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;

 -----------------------------   //3、修正T_ZQYE_HIS相关信息----------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZqyeUpdate', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZqyeUpdate', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH);
    l_sqlBuf :=
                "SELECT ZQYE.TZZH_ID," ||
                "         ZQYE.KHH," ||
                "         ZQYE.JYS," ||
                "         ZQYE.GDH," ||
                "         ZQYE.ZQDM," ||
                "         (CASE" ||
                "           WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND " || --手工调账部分
                "                D.JYLB_QS = '98' THEN" ||
                "            CBJS.KCRQ" ||
                "           WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND " || --分级基金部分
                "                D.KHH IS NOT NULL THEN" ||
                "            CBJS.KCRQ" ||
                "           ELSE" ||
                "            ZQYE.KCRQ" ||
                "         END) AS KCRQ," ||
                "         ZQYE.ZQLB," ||
                "         ZQYE.BZ," ||
                "         ZQYE.ZQSL," ||
                "         ZQYE.FLTSL," ||
                "         ZQYE.ZXSZ," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 CBJS.CCCB_TZ" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 CBJS.CCCB_TZ" ||
                "                ELSE" ||
                "                 ZQYE.CCCB" ||
                "              END) AS DECIMAL(16, 2)) AS CCCB," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 IF(ZQYE.ZQSL = 0, 0, ROUND(CBJS.CCCB_TZ / ZQYE.ZQSL, 4))" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 IF(ZQYE.ZQSL = 0, 0, ROUND(CBJS.CCCB_TZ / ZQYE.ZQSL, 4))" ||
                "                ELSE" ||
                "                 ZQYE.CBJ" ||
                "              END) AS DECIMAL(9, 4)) AS CBJ," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 CBJS.LJYK_TZ" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 CBJS.LJYK_TZ" ||
                "                ELSE" ||
                "                 ZQYE.LJYK" ||
                "              END) AS DECIMAL(16, 2)) AS LJYK," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 CBJS.CCCB_TZ - CBJS.LJYK_TZ" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 CBJS.CCCB_TZ - CBJS.LJYK_TZ" ||
                "                ELSE" ||
                "                 ZQYE.TBCCCB" ||
                "              END) AS DECIMAL(16, 2)) AS TBCCCB," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 IF(ZQYE.ZQSL = 0," ||
                "                    0," ||
                "                    ROUND((CBJS.CCCB_TZ - CBJS.LJYK_TZ) / ZQYE.ZQSL, 4))" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 IF(ZQYE.ZQSL = 0," ||
                "                    0," ||
                "                    ROUND((CBJS.CCCB_TZ - CBJS.LJYK_TZ) / ZQYE.ZQSL, 4))" ||
                "                ELSE" ||
                "                 ZQYE.TBCBJ" ||
                "              END) AS DECIMAL(16, 4)) AS TBCBJ," ||
                "         CAST((CASE" ||
                "                WHEN CBJS.KHH IS NOT NULL AND D.KHH IS NOT NULL AND" ||
                "                     D.JYLB_QS = '98' THEN" ||
                "                 0" ||
                "                WHEN CBJS.KHH IS NOT NULL AND FJJJ.JJDM IS NOT NULL AND" ||
                "                     D.KHH IS NOT NULL THEN" ||
                "                 ZQYE.ZXSZ - CBJS.SRZQSZ -" ||
                "                 ((CBJS.DRMRJE - CBJS.DRHBCFJE_ZR +" ||
                "                 NVL(CBJS.DRHBCFJE_ZR_TZ, 0) - NVL(CBJS.YK_ZR, 0)) -" ||
                "                 (CBJS.DRMCJE - CBJS.DRHBCFJE_ZC +" ||
                "                 NVL(CBJS.DRHBCFJE_ZC_TZ, 0) - NVL(CBJS.YK_ZC, 0)))" ||
                "                ELSE" ||
                "                 ZQYE.DRYK" ||
                "              END) AS DECIMAL(16, 2)) AS DRYK," ||
                "         ZQYE.ZQSL_ORIG " ||
                "    FROM " || F_IDS_GET_TABLENAME('sparkZqyeResult', I_KHH) ||" ZQYE" ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkCbjsResultYK', I_KHH) ||" CBJS" ||
                "      ON (ZQYE.KHH = CBJS.KHH AND" ||
                "         ZQYE.TZZH_ID = CBJS.TZZH_ID AND ZQYE.GDH = CBJS.GDH AND" ||
                "         ZQYE.JYS = CBJS.JYS AND ZQYE.ZQDM = CBJS.ZQDM)" ||
                "    LEFT JOIN cust.v_fjjjxx FJJJ" ||
                "      ON (ZQYE.JYS = FJJJ.JYS AND ZQYE.ZQDM = FJJJ.JJDM)" ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkCbjsExt', I_KHH) || " D" ||
                "      ON (ZQYE.TZZH_ID = D.TZZH_ID AND ZQYE.GDH = D.GDH AND" ||
                "         ZQYE.JYS = D.JYS AND ZQYE.ZQDM = D.ZQDM AND ZQYE.KHH = D.KHH)";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;


 ----------------------------------//折算变更        ---------------------------------- ----------------
 --- ---------------------------------- //4、筛选发生定、上、下折的母基交割流水 ------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZsbg', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZsbg', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZsbg', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZsbg', I_KHH);
    l_sqlBuf :=
                "SELECT" ||
                "   T.CJRQ AS RQ,T.TZZH_ID,T.KHH,T.KHXM,T.GDH,T.JYS,T.BZ,T.MJDM,F.MJMC,0 AS SRMJSL,0 AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ," ||
                "   0 AS MJLJYK_XZ,F.AJDM,F.AJMC,0 AS SRAJSL,0 AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,F.BJDM,F.BJMC,0 AS SRBJSL,0 AS SRBJTBCB," ||
                "   0 AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG " ||
                " FROM " ||
                "(SELECT j.cjrq as CJRQ," ||
                "       j.TZZH_ID," ||
                "       j.KHH," ||
                "       j.KHXM," ||
                "       j.GDH," ||
                "       j.JYS," ||
                "       j.BZ," ||
                "       X.MJDM" ||
                "  from " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) ||" j, cust.v_fjjjxx X" ||
                " where j.jylb in ('18', '19')" ||
                "   and j.jys = X.jys" ||
                "   and j.zqdm = X.jjdm " ||
                "   and cjbh in ('折算变更', '比例变更')" ||
                "   and j.ZQLB = 'L0'" ||
                " group by j.cjrq, j.TZZH_ID,j.KHH,j.KHXM,j.GDH,j.JYS,j.BZ,X.MJDM)T LEFT JOIN INFO.tfjjjxx  F " ||
                "    ON (T.JYS = F.JYS AND T.MJDM = F.MJDM)";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

 ----------------------------        //5、筛选上日数量、摊薄成本，筛选当日证券余额------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZsbgZqyeSR', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZsbgZqyeSR', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZsbgZqyeSR', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZsbgZqyeSR', I_KHH);
    l_sqlBuf :=
                 "Select " ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,NULL AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,NULL AS MJMC,0 AS SRMJSL," ||
                " B.ZQSL AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,NULL AS AJDM,NULL AS AJMC," ||
                " 0 AS SRAJSL,0 AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,NULL AS BJDM,NULL AS BJMC," ||
                " 0 AS SRBJSL,0 AS SRBJTBCB,0 AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG  " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH) ||"  B " ||  --筛选当日母基证券余额
                " where A.jys = B.jys" ||
                "   and A.mjdm = B.zqdm" ||
                "   and A.gdh = B.gdh" ||
                " UNION ALL" ||
                " select" ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,NULL AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,NULL AS MJMC,0 AS SRMJSL," ||
                " 0 AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,A.AJDM AS AJDM,NULL AS AJMC," ||
                " 0 AS SRAJSL,0 AS SRAJTBCB,B.ZQSL AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,NULL AS BJDM,NULL AS BJMC," ||
                " 0 AS SRBJSL,0 AS SRBJTBCB,0 AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG  " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH) ||" B " ||  --筛选当日A基证券余额
                " where A.JYS = B.JYS" ||
                "   and A.AJDM = B.ZQDM" ||
                "   and A.GDH = B.GDH" ||
                " UNION ALL" ||
                " select" ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,NULL AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,NULL AS MJMC,0 AS SRMJSL," ||
                " 0 AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,NULL AS AJDM,NULL AS AJMC," ||
                " 0 AS SRAJSL,0 AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,A.BJDM AS BJDM,NULL AS BJMC," ||
                " 0 AS SRBJSL,0 AS SRBJTBCB,B.ZQSL AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG  " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH) ||" B " ||  --筛选当日B基证券余额
                " where A.JYS = B.JYS" ||
                "   and A.BJDM = B.ZQDM" ||
                "   and A.GDH = B.GDH" ||
                " UNION ALL" ||
                " select" ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,CAST(NULL AS STRING) AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,CAST(NULL AS STRING) AS MJMC,B.ZQSL AS SRMJSL," ||
                " 0 AS DRMJSL,CAST(NVL(B.CCCB,0) AS DECIMAL(12,2)) AS SRMJTBCB,0 AS DRMJTBCB,CAST(NVL(B.CBJ,0) AS DECIMAL(9,4)) AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,NULL AS AJDM,CAST(NULL AS STRING) AS AJMC," ||
                " 0 AS SRAJSL,0 AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,NULL AS BJDM,CAST(NULL AS STRING) AS BJMC," ||
                " 0 AS SRBJSL,0 AS SRBJTBCB,0 AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG  " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) ||" B " ||  --筛选上日母基数量、摊薄成本
                " where A.JYS = B.JYS" ||
                "   and A.MJDM = B.ZQDM" ||
                "   and A.GDH = B.GDH" ||
                " UNION ALL " ||
                " select" ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,NULL AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,NULL AS MJMC,0 AS SRMJSL," ||
                " 0 AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,A.AJDM AS AJDM,NULL AS AJMC," ||
                " B.ZQSL AS SRAJSL,CAST(NVL(B.CCCB,0) AS DECIMAL(12,2)) AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,CAST(NVL(B.CBJ,0) AS DECIMAL(9,4)) AS SRAJTBCBJ,0 AS DRAJTBCBJ,NULL AS BJDM,NULL AS BJMC," ||
                " 0 AS SRBJSL,0 AS SRBJTBCB,0 AS DRBJSL,0 AS DRBJTBCB,0 AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG   " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) ||" B " ||  --筛选上日A基数量、摊薄成本
                " where A.JYS = B.JYS" ||
                "   and A.AJDM = B.ZQDM" ||
                "   and A.GDH = B.GDH" ||
                " UNION ALL" ||
                " select" ||
                " A.RQ AS RQ,0 AS TZZH_ID,A.KHH AS KHH,NULL AS KHXM,A.GDH AS GDH,A.JYS AS JYS,A.BZ AS BZ,A.MJDM AS MJDM,NULL AS MJMC,0 AS SRMJSL," ||
                " 0 AS DRMJSL,0 AS SRMJTBCB,0 AS DRMJTBCB,0 AS SRMJTBCBJ,0 AS DRMJTBCBJ,0 AS MJLJYK_XZ,NULL AS AJDM,NULL AS AJMC," ||
                " 0 AS SRAJSL,0 AS SRAJTBCB,0 AS DRAJSL,0 AS DRAJTBCB,0 AS SRAJTBCBJ,0 AS DRAJTBCBJ,A.BJDM AS BJDM,NULL AS BJMC," ||
                " B.ZQSL AS SRBJSL,CAST(NVL(B.CCCB,0) AS DECIMAL(12,2)) AS SRBJTBCB,0 AS DRBJSL,0 AS DRBJTBCB,CAST(NVL(B.CBJ,0) AS DECIMAL(9,4)) AS SRBJTBCBJ,0 AS DRBJTBCBJ,0 AS FLAG  " ||
                "  from " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" A, " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) ||" B " ||  --筛选上日B基数量、摊薄成本
                " where A.JYS = B.JYS" ||
                "   and A.BJDM = B.ZQDM" ||
                "   and A.GDH = B.GDH";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

 ---------------------------     //6、折算变更-定折-----------------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZsbgDZ', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZsbgDZ', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZsbgDZ', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZsbgDZ', I_KHH);
    l_sqlBuf :=
                "SELECT " ||
                "    T.RQ AS RQ,T.TZZH_ID AS TZZH_ID,T.KHH AS KHH,T.KHXM AS KHXM,T.GDH AS GDH,T.JYS AS JYS,T.BZ AS BZ," ||
                "    T.MJDM AS MJDM,T.MJMC AS MJMC,NVL(TMP.SRMJSL,T.SRMJSL) AS SRMJSL,NVL(TMP.DRMJSL,T.DRMJSL) AS DRMJSL," ||
                "    NVL(TMP.SRMJTBCB,T.SRMJTBCB) AS SRMJTBCB,NVL(TMP.DRMJTBCB,T.DRMJTBCB) AS DRMJTBCB," ||
                "    NVL(TMP.SRMJTBCBJ,T.SRMJTBCBJ) AS SRMJTBCBJ,NVL(TMP.DRMJTBCBJ,T.DRMJTBCBJ) AS DRMJTBCBJ," ||
                "    T.MJLJYK_XZ AS MJLJYK_XZ,T.AJDM AS AJDM,T.AJMC AS AJMC,NVL(TMP.SRAJSL,T.SRAJSL) AS SRAJSL," ||
                "    NVL(TMP.SRAJTBCB,T.SRAJTBCB) AS SRAJTBCB,NVL(TMP.DRAJSL,T.DRAJSL) AS DRAJSL," ||
                "    NVL(TMP.DRAJTBCB,T.DRAJTBCB) AS DRAJTBCB,NVL(TMP.SRAJTBCBJ,T.SRAJTBCBJ) AS SRAJTBCBJ," ||
                "    NVL(TMP.DRAJTBCBJ,T.DRAJTBCBJ) AS DRAJTBCBJ,T.BJDM AS BJDM," ||
                "    T.BJMC AS BJMC,NVL(TMP.SRBJSL,T.SRBJSL) AS SRBJSL,NVL(TMP.SRBJTBCB,T.SRBJTBCB) AS SRBJTBCB," ||
                "    NVL(TMP.DRBJSL,T.DRBJSL) AS DRBJSL,NVL(TMP.DRBJTBCB,T.DRBJTBCB) AS DRBJTBCB," ||
                "    NVL(TMP.SRBJTBCBJ,T.SRBJTBCBJ) AS SRBJTBCBJ,NVL(TMP.DRBJTBCBJ,T.DRBJTBCBJ) AS DRBJTBCBJ," ||
                "    (CASE WHEN J2.JJDM IS NOT NULL AND J2.JJJZ >= J2.SZJZ AND J2.SZJZ > 0 THEN 2 " || -- T触发日母基净值高于上折净值，则上折
                "          WHEN J2.JJDM IS NOT NULL AND J2.JJJZ <= J2.XZJZ THEN 1 " || -- T触发日B基净值低于下折净值，则下折
                "          ELSE T.FLAG" ||
                "    END) AS FLAG" ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkZsbg', I_KHH) ||" T" ||
                "  LEFT JOIN (select RQ," ||
                "                    JYS," ||
                "                    TZZH_ID," ||
                "                    KHH," ||
                "                    GDH," ||
                "                    BZ," ||
                "                    MJDM," ||
                "                    sum(NVL(tmp.SRMJSL, 0)) as SRMJSL," ||
                "                    SUM(NVL(tmp.DRMJSL, 0)) as DRMJSL," ||
                "                    SUM(NVL(tmp.SRMJTBCB, 0)) as SRMJTBCB," ||
                "                    SUM(NVL(tmp.DRMJTBCB, 0)) as DRMJTBCB," ||
                "                    SUM(NVL(tmp.SRMJTBCBJ, 0)) as SRMJTBCBJ," ||
                "                    SUM(NVL(tmp.DRMJTBCBJ, 0)) as DRMJTBCBJ," ||
                "                    SUM(NVL(tmp.SRAJSL, 0)) as SRAJSL," ||
                "                    SUM(NVL(tmp.SRAJTBCB, 0)) as SRAJTBCB," ||
                "                    SUM(NVL(tmp.DRAJSL, 0)) as DRAJSL," ||
                "                    SUM(NVL(tmp.DRAJTBCB, 0)) as DRAJTBCB," ||
                "                    SUM(NVL(tmp.SRAJTBCBJ, 0)) as SRAJTBCBJ," ||
                "                    SUM(NVL(tmp.DRAJTBCBJ, 0)) as DRAJTBCBJ," ||
                "                    SUM(NVL(tmp.SRBJSL, 0)) as SRBJSL," ||
                "                    SUM(NVL(tmp.SRBJTBCB, 0)) as SRBJTBCB," ||
                "                    SUM(NVL(tmp.DRBJSL, 0)) as DRBJSL," ||
                "                    SUM(NVL(tmp.DRBJTBCB, 0)) as DRBJTBCB," ||
                "                    SUM(NVL(tmp.SRBJTBCBJ, 0)) as SRBJTBCBJ," ||
                "                    SUM(NVL(tmp.DRBJTBCBJ, 0)) as DRBJTBCBJ" ||
                "               from " || F_IDS_GET_TABLENAME('sparkZsbgZqyeSR', I_KHH) ||" tmp" ||
                "              group by RQ, JYS, TZZH_ID, KHH, GDH, BZ, MJDM)TMP" ||
                "        ON (T.RQ=TMP.RQ AND T.MJDM=TMP.MJDM AND T.JYS=TMP.JYS AND T.TZZH_ID=TMP.TZZH_ID AND T.KHH=TMP.KHH AND T.GDH=TMP.GDH AND T.BZ=TMP.BZ)" ||
                "  LEFT JOIN (SELECT J2.TADM,J2.JJDM,J2.JJJZ,X.XZJZ,X.SZJZ FROM " || F_IDS_GET_TABLENAME('sparkJjjzTpre2', I_KHH) ||" J2,INFO.TFJJJXX X " ||  --标识定、上、下折
                "                WHERE j2.tadm = x.JYS and j2.jjdm = x.BJDM)J2" ||
                "        ON (T.JYS=J2.TADM AND T.BJDM=J2.JJDM)";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

 ----------------------------- //7、计算定折----------------------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZsbgResult', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZsbgResult', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH);
    l_sqlBuf :=
                " SELECT " ||
                "T.TZZH_ID," ||
                "T.KHH," ||
                "T.KHXM," ||
                "T.GDH," ||
                "T.JYS," ||
                "T.BZ," ||
                "T.MJDM," ||
                "T.MJMC," ||
                " CAST(T.SRMJSL AS INT) AS SRMJSL," ||
                " CAST(T.DRMJSL AS INT) AS DRMJSL," ||
                " CAST(T.SRMJTBCB AS DECIMAL(16,2)) AS SRMJTBCB," ||
                " CAST(CASE WHEN T.FLAG=0 THEN " ||
                "        T.SRMJTBCB+T.SRAJTBCB-T.DRAJTBCB+IF(NVL(T.DRBJTBCB,0)>0,T.SRBJTBCB-T.DRBJTBCB,0)" ||
                "     WHEN T.JJDM IS NOT NULL AND T.FLAG = 1 THEN" ||
                "        t.SRMJTBCB + NVL((t.SRAJSL * NVL(T.fhps, 1.0) - t.DRAJSL * 1), 0)" ||
                "     WHEN T.FLAG=2 THEN" ||
                "        t.SRMJTBCB + t.SRAJTBCB - t.DRAJTBCB + t.SRBJTBCB - t.DRBJTBCB" ||
                "     ELSE T.DRMJTBCB" ||
                " END AS DECIMAL(16,2)) AS DRMJTBCB," ||
                " CAST(T.SRMJTBCBJ AS DECIMAL(9,4)) AS SRMJTBCBJ," ||
                " CAST(CASE WHEN T.FLAG=0 THEN " ||
                "        (T.SRMJTBCB+T.SRAJTBCB-T.DRAJTBCB+IF(NVL(T.DRBJTBCB,0)>0,T.SRBJTBCB-T.DRBJTBCB,0))/T.DRMJSL" ||
                "     WHEN T.JJDM IS NOT NULL AND T.FLAG = 1 THEN" ||
                "        if(t.DRMJSL=0, 0, (t.SRMJTBCB + NVL(t.SRAJSL * NVL(T.fhps, 1.0) - t.DRAJSL * 1, 0)) / t.DRMJSL)" ||
                "     WHEN T.FLAG=2 THEN" ||
                "        (t.SRMJTBCB + t.SRAJTBCB - t.DRAJTBCB + t.SRBJTBCB - t.DRBJTBCB) / t.DRMJSL" ||
                "     ELSE T.DRMJTBCBJ" ||
                " END AS DECIMAL(9,4)) AS DRMJTBCBJ," ||
                " T.MJLJYK_XZ," ||
                " T.AJDM," ||
                " T.AJMC," ||
                " CAST(T.SRAJSL AS INT) AS SRAJSL," ||
                " CAST(T.SRAJTBCB AS DECIMAL(12,2)) AS SRAJTBCB," ||
                " CAST(T.DRAJSL AS INT) AS DRAJSL," ||
                " CAST(T.DRAJTBCB AS DECIMAL(12,2)) AS DRAJTBCB," ||
                " CAST(T.SRAJTBCBJ AS DECIMAL(9,4)) AS SRAJTBCBJ," ||
                " CAST(T.DRAJTBCBJ AS DECIMAL(9,4)) AS DRAJTBCBJ," ||
                "T.BJDM," ||
                "T.BJMC," ||
                "CAST(T.SRBJSL AS INT) AS SRBJSL," ||
                "CAST(T.SRBJTBCB AS DECIMAL(12,2)) AS SRBJTBCB," ||
                "CAST(T.DRBJSL AS INT) AS DRBJSL," ||
                "CAST(T.DRBJTBCB AS DECIMAL(12,2)) AS DRBJTBCB," ||
                "CAST(T.SRBJTBCBJ AS DECIMAL(9,4)) AS SRBJTBCBJ," ||
                "CAST(T.DRBJTBCBJ AS DECIMAL(9,4)) AS DRBJTBCBJ," ||
                "T.FLAG," ||
                I_RQ || " as rq" ||
                " FROM" ||
                "(SELECT T.RQ," ||
                "       T.TZZH_ID," ||
                "       T.KHH," ||
                "       T.KHXM," ||
                "       T.GDH," ||
                "       T.JYS," ||
                "       T.BZ," ||
                "       T.MJDM," ||
                "       T.MJMC," ||
                "       T.SRMJSL," ||
                "       T.DRMJSL," ||
                "       T.SRMJTBCB," ||
                "       T.DRMJTBCB," ||
                "       T.SRMJTBCBJ," ||
                "       T.DRMJTBCBJ," ||
                "       T.MJLJYK_XZ," ||
                "       T.AJDM," ||
                "       T.AJMC," ||
                "       T.SRAJSL," ||
                "       T.SRAJTBCB," ||
                "       T.DRAJSL," ||
                "       (CASE WHEN JA.JJDM IS NOT NULL AND T.FLAG=0 THEN " ||
                "            T.SRAJTBCB-T.SRAJSL*(NVL(JA.fhps,1.0)-1)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=1 THEN" ||
                "            NVL(T.SRAJTBCB-(T.SRAJSL*NVL(jA.fhps,1.0)-T.DRAJSL*1),0)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=2 THEN" ||
                "            case when NVL(jA.fhps, 0) > 0 then " ||
                "        t.SRAJTBCB - (t.SRAJSL * (NVL(jA.fhps, 1.0)-1))" ||
                "      else " ||
                "        t.SRAJTBCB " || --上折时有可能母基不参与(fhps为0或为null)
                "      end" ||
                "             ELSE T.DRAJTBCB" ||
                "        END) AS DRAJTBCB," ||
                "       T.SRAJTBCBJ," ||
                "       (CASE WHEN JA.JJDM IS NOT NULL AND T.FLAG=0 THEN " ||
                "            IF(T.DRAJSL=0, 0,(T.SRAJTBCB-T.SRAJSL*(NVL(JA.fhps,1.0)-1))/T.DRAJSL)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=1 THEN" ||
                "            IF(T.DRAJSL=0,0,(T.SRAJTBCB-(T.SRAJSL*NVL(JA.fhps,1.0)-T.DRAJSL*1))/T.DRAJSL)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=2 THEN" ||
                "            case when NVL(jA.fhps, 0) > 0 then " ||
                "        if(t.DRAJSL > 0, (t.SRAJTBCB - t.SRAJSL * (NVL(jA.fhps, 1.0)-1)) / t.DRAJSL,0)" ||
                "      else " ||
                "        t.SRAJTBCBJ " ||  --上折时有可能母基不参与(fhps为0或为null)
                "      end" ||
                "             ELSE T.DRAJTBCBJ" ||
                "        END) AS DRAJTBCBJ," ||
                "       T.BJDM," ||
                "       T.BJMC," ||
                "       T.SRBJSL," ||
                "       T.SRBJTBCB," ||
                "       T.DRBJSL," ||
                "       (CASE WHEN JB.JJDM IS NOT NULL AND T.FLAG=0 THEN" ||
                "            IF(NVL(JB.fhps,1)>=1,T.SRBJTBCB-T.SRBJSL*(NVL(JB.fhps,1)-1),T.SRBJTBCB)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=1 THEN" ||
                "            T.SRBJTBCB" ||
                "             WHEN JB.JJDM IS NOT NULL AND T.FLAG=2 THEN" ||
                "            t.SRBJTBCB - (t.SRBJSL * (NVL(jB.fhps, 1.0)-1))" ||
                "             ELSE T.DRBJTBCB" ||
                "        END) AS DRBJTBCB," ||
                "       T.SRBJTBCBJ," ||
                "       (CASE WHEN JB.JJDM IS NOT NULL AND T.FLAG=0 THEN" ||
                "            IF(T.DRBJSL=0,0,(IF(NVL(JB.fhps,1)>=1,T.SRBJTBCB-T.SRBJSL*(NVL(JB.fhps,1)-1),T.SRBJTBCB))/T.DRBJSL)" ||
                "             WHEN JA.JJDM IS NOT NULL AND T.FLAG=1 THEN" ||
                "            case when t.DRBJSL > 0 then t.SRBJTBCB / t.DRBJSL else 0 end" ||
                "             WHEN JB.JJDM IS NOT NULL AND T.FLAG=2 THEN" ||
                "            if(t.DRBJSL = 0, 0, (t.SRBJTBCB - t.SRBJSL * (NVL(jB.fhps, 1.0)-1))/t.DRBJSL)" ||
                "             ELSE T.DRBJTBCBJ" ||
                "        END) AS DRBJTBCBJ," ||
                "       T.FLAG," ||
                "       JA.JJDM," ||
                "       JA.FHPS" ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkZsbgDZ', I_KHH) ||" T" ||
                "  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkJjjzTpre1', I_KHH) ||" JA" ||
                "    ON (T.AJDM=JA.JJDM AND T.JYS=JA.TADM) " ||  --计算A基
                "  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkJjjzTpre1', I_KHH) ||" JB" ||
                "    ON (T.BJDM=JB.JJDM AND T.JYS=JB.TADM AND NVL(JB.FHPS,0)>0) " ||  --增加B基定折处理
                " )T";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_ZSBG_HIS", I_RQ, I_KHH);
  END;



 -------------------------  //8、创建折算变更代码临时表，后续多处使用------------------------------

   BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZsbgZqdm', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZsbgZqdm', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZsbgZqdm', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZsbgZqdm', I_KHH);
    l_sqlBuf :=
                "SELECT TZZH_ID, KHH, GDH, JYS, MJDM AS ZQDM  " ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" T  " ||
                " UNION ALL  " ||
                " SELECT TZZH_ID, KHH, GDH, JYS, AJDM AS ZQDM  " ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" T  " ||
                " UNION ALL  " ||
                " SELECT TZZH_ID, KHH, GDH, JYS, BJDM AS ZQDM  " ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" T ";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

 -------------------------------  //9、生成证券余额-成本计算---------------------------------------------
   BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkCbjsResultNew', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkCbjsResultNew', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH);
    l_sqlBuf :=
                "SELECT   " ||
                "T.TZZH_ID,  " ||
                "T.KHH,  " ||
                "T.GDH,  " ||
                "T.JYS,  " ||
                "T.ZQDM,  " ||
                "T.ZQLB,  " ||
                "T.BZ,  " ||
                "(CASE WHEN Z.KHH IS NOT NULL AND (T.KCRQ = 0 OR T.KCRQ IS NULL) THEN " || I_RQ ||
                "      ELSE T.KCRQ  " ||
                " END) AS KCRQ,  " ||
                "T.ZQSL,  " ||
                "T.ZQSZ,  " ||
                "T.DRYSSL,  " ||
                "T.DRMRJE,  " ||
                "T.DRMRSL,  " ||
                "T.DRPGJE,  " ||
                "T.DRPGSL,  " ||
                "T.DRMCJE,  " ||
                "T.DRMCSL,  " ||
                "T.DRJYFY,  " ||
                "T.SRCCJJ,  " ||
                "T.LJMRJE,  " ||
                "T.LJMRSL,  " ||
                "T.LJPGJE,  " ||
                "T.LJPGSL,  " ||
                "T.LJMCJE,  " ||
                "T.LJMCSL,  " ||
                "T.LJJYFY,  " ||
                "T.SRZQSL,  " ||
                "T.SRZQSZ,  " ||
                "T.SRCBJ,  " ||
                "T.SRCCCB,  " ||
                "T.SRLJYK,  " ||
                "CAST((CASE WHEN ZA.KHH IS NOT NULL THEN  " ||
                "        NVL(T.CCCB, T.SRCCCB) - (ZA.SRAJTBCB - ZA.DRAJTBCB)  " ||
                "      WHEN ZB.KHH IS NOT NULL AND ZB.FLAG IN (0,2) AND NVL(ZA.DRBJTBCB,0) > 0 THEN  " ||
                "        NVL(T.CCCB, T.SRCCCB) - (ZB.SRBJTBCB - ZB.DRBJTBCB)  " ||
                "      WHEN ZM.KHH IS NOT NULL THEN  " ||
                "        NVL(T.CCCB,T.SRCCCB)+ZM.SRAJTBCB-ZM.DRAJTBCB+CASE WHEN ZM.FLAG in(0,2) and NVL(ZM.DRBJTBCB,0)>0 THEN ZM.SRBJTBCB-ZM.DRBJTBCB ELSE 0 END  " ||
                "      ELSE T.CCCB  " ||
                " END) AS DECIMAL(16,2)) AS CCCB,  " ||
                "T.LJYK,  " ||
                "T.CCCB_TZ,  " ||
                "T.LJYK_TZ,  " ||
                "T.DRHBCFJE_ZR,  " ||
                "T.DRHBCFSL_ZR,  " ||
                "T.DRHBCFJE_ZC,  " ||
                "T.DRHBCFSL_ZC,  " ||
                "T.DRHBCFJE_ZR_TZ,  " ||
                "T.DRHBCFJE_ZC_TZ,  " ||
                "T.YK_ZR,  " ||
                "T.YK_ZC,  " ||
                "T.DRSHDQSL,  " ||
                "T.DRSHDQJE,  " ||
                "T.DRSHSYSL,  " ||
                "T.SRSHSYSL,  " ||
                "T.QCRQ,  " ||
                "T.DRSHSL_ETFHB,  " ||
                "T.DRSGSL_ETFHB,  " ||
                "T.DRSHJE_SJ_ETFHB,  " ||
                "T.DRSGJE_SJ_ETFHB,  " ||
                "T.DRSHJE_SS_ETFHB,  " ||
                "T.DRSGJE_SF_ETFHB,  " ||
                "T.LJSHJE_SJ_ETFHB,  " ||
                "T.LJSGJE_SJ_ETFHB,  " ||
                "T.LJSHJE_SS_ETFHB,  " ||
                "T.LJSGJE_SF_ETFHB,  " ||
                I_RQ || " as rq" ||
                " FROM  " ||
                "  " || F_IDS_GET_TABLENAME('sparkCbjsResultYK', I_KHH) ||" T   " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZA  " ||
                "ON (T.ZQDM = ZA.AJDM AND T.JYS = ZA.JYS AND T.GDH = ZA.GDH AND T.KHH = ZA.KHH AND T.BZ = ZA.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZB  " ||
                "ON (T.ZQDM = ZB.BJDM AND T.JYS = ZB.JYS AND T.GDH = ZB.GDH AND T.KHH = ZB.KHH AND T.BZ = ZB.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZM  " ||
                "ON (T.ZQDM = ZM.MJDM AND T.JYS = ZM.JYS AND T.GDH = ZM.GDH AND T.KHH = ZM.KHH AND T.BZ = ZM.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgZqdm', I_KHH) ||" Z  " ||
                "ON (T.ZQDM = Z.ZQDM AND T.JYS = Z.JYS AND T.GDH = Z.GDH AND T.KHH = Z.KHH)";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

------------        //10、更新到目标表-------------------------
-------------------- //10、1成本盈亏---------------------------
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */

    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "t_zqye_cbjs", I_RQ, I_KHH);
  END;



 -----------------------     //20、2证券余额---------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZqyeResultNew', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZqyeResultNew', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZqyeResultNew', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeResultNew', I_KHH);
    l_sqlBuf :=
                " SELECT   " ||
                " T.TZZH_ID,  " ||
                "T.KHH,  " ||
                "T.JYS,  " ||
                "T.GDH,  " ||
                "T.ZQDM,  " ||
                "(CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN Z.KCRQ ELSE T.KCRQ END) AS KCRQ,  " ||
                "T.ZQLB,  " ||
                "T.BZ,  " ||
                "T.ZQSL,  " ||
                "T.FLTSL,  " ||
                "T.ZXSZ,  " ||
                "CAST((CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN Z.CCCB ELSE T.CCCB END) AS DECIMAL(16,2)) AS CCCB,  " ||
                "CAST((CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN   " ||
                "        case when t.ZQSL <> 0 then z.CCCB / t.ZQSL else 0 end  " ||
                "      ELSE T.CBJ  " ||
                " END) AS DECIMAL(9,4)) AS CBJ,  " ||
                "CAST((CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN Z.LJYK ELSE T.LJYK END) AS DECIMAL(16,2)) AS LJYK,  " ||
                "CAST((CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN Z.CCCB - Z.LJYK ELSE T.TBCCCB END) AS DECIMAL(16,2)) AS TBCCCB,  " ||
                "CAST((CASE WHEN Z.KHH IS NOT NULL AND X.KHH IS NOT NULL THEN   " ||
                "        case when t.ZQSL <> 0 then (z.CCCB - z.LJYK) / t.ZQSL else 0 end  " ||
                "      ELSE T.TBCBJ  " ||
                " END) AS DECIMAL(16,4)) AS TBCBJ,  " ||
                "CAST((CASE WHEN ZM.KHH IS NOT NULL THEN  " ||
                "        NVL(t.DRYK,0)-(ZM.SRAJTBCB-ZM.DRAJTBCB)-case when ZM.flag in(0, 2) and NVL(ZM.DRBJTBCB,0)>0 then ZM.SRBJTBCB-ZM.DRBJTBCB else 0 end  " ||
                "      WHEN ZA.KHH IS NOT NULL AND S.KHH IS NOT NULL THEN  " ||
                "        ZA.SRAJTBCB - ZA.DRAJTBCB + T.ZXSZ - S.ZXSZ  " ||
                "      WHEN ZB.KHH IS NOT NULL THEN  " ||
                "        0  " ||
                "      ELSE T.DRYK  " ||
                " END) AS DECIMAL(16,2)) AS DRYK,  " ||
                " T.ZQSL_ORIG,  " ||
                I_RQ || " as rq " ||
                " FROM   " ||
                " " || F_IDS_GET_TABLENAME('sparkZqyeUpdate', I_KHH) ||" T   " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" Z   " ||--同步买入持仓成本、成本价、累计盈亏
                "        ON (T.ZQDM = Z.ZQDM AND T.JYS = Z.JYS AND T.GDH = Z.GDH AND T.KHH = Z.KHH AND T.BZ = Z.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgZqdm', I_KHH) ||" X   " || --同步买入持仓成本、成本价、累计盈亏
                "        ON (T.ZQDM = X.ZQDM AND T.JYS = X.JYS AND T.GDH = X.GDH AND T.KHH = X.KHH)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZM   " ||  --修正母基当日盈亏
                "        ON (T.ZQDM = ZM.MJDM AND T.JYS = ZM.JYS AND T.GDH = ZM.GDH AND T.KHH = ZM.KHH AND T.BZ = ZM.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZA   " || --修正A基当日盈亏
                "        ON (T.ZQDM = ZA.AJDM AND T.JYS = ZA.JYS AND T.GDH = ZA.GDH AND T.KHH = ZA.KHH AND T.BZ = ZA.BZ)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) ||" S  " ||
                "        ON (T.KHH = S.KHH AND T.JYS = S.JYS AND T.GDH = S.GDH AND T.ZQDM = S.ZQDM)  " ||
                "    LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" ZB   " || --修正B基当日盈亏
                "        ON (T.ZQDM = ZB.BJDM AND T.JYS = ZB.JYS AND T.GDH = ZB.GDH AND T.KHH = ZB.KHH AND T.BZ = ZB.BZ AND  " ||
                "            ZB.FLAG IN (0,2) AND NVL(ZB.DRBJTBCB,0) > 0 )  ";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

   BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */

    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_ZQYE_HIS", I_RQ, I_KHH);
  END;


 -------------------------------  //10、3交割清算-------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJgmxlsQSResultNew', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJgmxlsQSResultNew', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJgmxlsQSResultNew', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQSResultNew', I_KHH);
    l_sqlBuf :=
                "SELECT   " ||
                "T.TZZH_ID,T.SEQNO,T.WTH,T.KHH,T.KHXM,T.GDH,T.JYS,T.BZ,T.YYB,T.ZQDM,T.ZQMC,T.ZQLB,  " ||
                "T.JYLB,T.CJBH,T.CJSJ,T.SBSJ,T.CJBS,T.CJSL,T.CJJG,T.JSJ,T.LXJG,T.CJJE,T.LXJE,  " ||
                "T.YSJE,T.JSRQ,T.BCZQYE,T.YSSL,T.ZXJ,T.JJJZ,T.SEQNO_QS,T.BCZQYE_QS,T.CJSJ_QS,T.CCCB_QS,  " ||
                "T.CBJ_QS,T.SXYK_QS,T.LJYK_QS,T.REMARK_QS,T.JYLB_QS,  " ||
                "(CASE WHEN T.JYLB IN ('18','19') AND T.CJBH IN ('折算变更', '比例变更') AND T.ZT_QS = 1 THEN 0 ELSE T.ZT_QS END) AS ZT_QS,  " ||
                "T.YK_ZC,T.YK_ZR,T.LSH,T.ROWNUM,T.S1,T.S2,T.S3,T.S4,T.S5,T.S6,T.JYFY,T.CCCB_ZC,T.CCCB_ZR,  " || I_RQ || " as cjrq " ||
                " FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH) ||" T   " ||
                " WHERE NOT(T.JYLB_QS IN ('18','19') AND T.CJBH IN ('定折', '下折', '上折') AND T.ZT_QS = 2)  " ||
                " UNION ALL  " ||
                " select  t.tzzh_id, 0 AS SEQNO, 0 AS WTH, t.khh, t.khxm, t.gdh, t.jys, t.bz,NULL AS YYB, t.bjdm AS ZQDM, t.bjmc AS ZQMC,  " ||
                "        NULL AS ZQLB,NULL AS JYLB,'下折',  " ||
                "    '00\:00\:00' AS CJSJ, '00\:00\:00' AS SBSJ,NULL AS CJBS, CAST(abs(t.drbjsl - t.srbjsl) AS INT) AS CJSL,  " ||
                "    NULL AS CJJG,NULL AS JSJ,NULL AS LXJG,NULL AS CJJE,NULL AS LXJE,NULL AS YSJE,  " ||
                "    CAST(" || I_RQ || " AS INT) AS JSRQ, NULL AS BCZQYE,NULL AS YSSL,NULL AS ZXJ,NULL AS JJJZ,NULL AS SEQNO_QS,  " ||
                "    t.drbjsl AS BCZQYE_QS, NULL AS CJSJ_QS,CAST(t.drbjtbcb AS DECIMAL(16,2)) AS CCCB_QS, CAST(t.DRBJTBCBJ AS DECIMAL(10,4)) AS CBJ_QS, 0 AS SXYK_QS, CAST(s.ljyk AS DECIMAL(16,2)) AS LJYK_QS,   " ||
                "    concat('下折,', concat('B基数量减少:', cast(abs(t.drbjsl-t.srbjsl) as STRING))) AS REMARK_QS,  " ||
                "    if(drbjsl > srbjsl, '18', '19') AS JYLB_QS, 2 AS ZT_QS,  " ||
                "    NULL AS YK_ZC,NULL AS YK_ZR,NULL AS LSH,NULL AS ROWNUM,NULL AS S1,NULL AS S2,NULL AS S3,NULL AS S4,  " ||
                "    NULL AS S5,NULL AS S6,NULL AS JYFY,NULL AS CCCB_ZC,NULL AS CCCB_ZR ,  " || I_RQ || " as cjrq " ||
                " from " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" t, " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" s  " ||
                " where t.tzzh_id = s.tzzh_id and t.khh = s.khh and t.jys = s.jys and t.bjdm = s.zqdm and t.flag = 1 and t.DRBJSL > 0  " ||
                " UNION ALL  " ||
                " select  t.tzzh_id, 0 AS SEQNO, 0 AS WTH, t.khh, t.khxm, t.gdh, t.jys, t.bz,NULL AS YYB, t.ajdm AS ZQDM, t.ajmc AS ZQMC,   " ||
                "        NULL AS ZQLB,NULL AS JYLB,'下折' AS CJBH,  " ||
                "    '00\:00\:00' AS CJSJ, '00\:00\:00' AS SBSJ,NULL AS CJBS, CAST(abs(t.drajsl - t.srajsl) AS INT) AS CJSL,   " ||
                "    NULL AS CJJG,NULL AS JSJ,NULL AS LXJG,NULL AS CJJE,NULL AS LXJE,NULL AS YSJE,  " ||
                "    CAST(" || I_RQ || " AS INT) AS JSRQ, NULL AS BCZQYE,NULL AS YSSL,NULL AS ZXJ,NULL AS JJJZ,NULL AS SEQNO_QS,  " ||
                "    t.drajsl AS BCZQYE_QS, NULL AS CJSJ_QS,CAST(t.drajtbcb AS DECIMAL(16,2))AS CCCB_QS, CAST(t.DRAJTBCBJ AS DECIMAL(10,4)) AS CBJ_QS, 0 AS SXYK_QS, CAST(s.ljyk AS DECIMAL(16,2)) AS LJYK_QS,   " ||
                "    concat('下折,', concat('A基数量减少:', cast(abs(t.drajsl-t.srajsl) as STRING))) AS REMARK_QS,  " ||
                "    if(drajsl > srajsl, '18', '19') AS JYLB_QS, 2 AS ZT_QS,  " ||
                "    NULL AS YK_ZC,NULL AS YK_ZR,NULL AS LSH,NULL AS ROWNUM,NULL AS S1,NULL AS S2,NULL AS S3,NULL AS S4,  " ||
                "    NULL AS S5,NULL AS S6,NULL AS JYFY,NULL AS CCCB_ZC,NULL AS CCCB_ZR  ,  " || I_RQ || " as cjrq " ||
                " from " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" t, " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" s  " ||
                " where t.tzzh_id = s.tzzh_id and t.khh = s.khh and t.jys = s.jys and t.ajdm = s.zqdm and t.flag = 1 and t.DRAJSL > 0  " ||
                " UNION ALL  " ||
                " select  t.tzzh_id, 0 AS SEQNO, 0 AS WTH, t.khh, t.khxm, t.gdh, t.jys, t.bz,NULL AS YYB, t.mjdm AS ZQDM,  " ||
                "        t.mjmc AS ZQMC,NULL AS ZQLB,NULL AS JYLB,  " ||
                "        (case when t.flag = 0 then '定折' when t.flag = 1 THEN '下折' else '上折' end) AS CJBH,  " ||
                "    '00\:00\:00' AS CJSJ, '00\:00\:00' AS SBSJ,NULL AS CJBS, CAST(abs(t.drmjsl - t.srmjsl) AS INT) AS CJSL,  " ||
                "    NULL AS CJJG,NULL AS JSJ,NULL AS LXJG,NULL AS CJJE,NULL AS LXJE,NULL AS YSJE,CAST(" || I_RQ || " AS INT) AS JSRQ,   " ||
                "    NULL AS BCZQYE,NULL AS YSSL,NULL AS ZXJ,NULL AS JJJZ,NULL AS SEQNO_QS,  " ||
                "    t.drmjsl AS BCZQYE_QS,NULL AS CJSJ_QS, CAST(t.drmjtbcb AS DECIMAL(16,2)) AS CCCB_QS, CAST(t.DRMJTBCBJ AS DECIMAL(10,4)) AS CBJ_QS, 0 AS SXYK_QS, CAST(s.ljyk AS DECIMAL(16,2)) AS LJYK_QS,   " ||
                "    case when t.flag = 0 then   " ||
                "        concat('定折, 来源(', if(t.DRAJTBCB < t.SRAJTBCB, concat('A基:', t.ajdm, t.ajmc), ''), if(t.drbjtbcb < t.srbjtbcb and NVL(t.drbjtbcb,0) > 0, concat(' B基:', t.bjdm, t.bjmc), ''), if(t.SRMJSL>0, cast(' M基自调整' as STRING), ''), ')')   " ||
                "      when t.flag = 1 THEN  " ||
                "        concat('下折, 来源(', if(t.DRAJTBCB < t.SRAJTBCB, concat('A基:', t.ajdm, t.ajmc), ''), if(t.srmjsl>0, cast(' M基自调整' as STRING), ''), ')')   " ||
                "      when t.flag = 2 then   " ||
                "        concat('上折, 来源(', if(t.DRAJTBCB < t.SRAJTBCB, concat('A基:', t.ajdm, t.ajmc), ''), if(t.drbjtbcb < t.srbjtbcb, concat(' B基:', t.bjdm, t.bjmc), ''), if(t.srmjsl>0, cast(' M基自调整' as STRING), ''), ')')    " ||
                "    end AS REMARK_QS,   " ||
                "    if(drmjsl > srmjsl, '18', '19') AS JYLB_QS, 2 AS ZT_QS,  " ||
                "    NULL AS YK_ZC,NULL AS YK_ZR,NULL AS LSH,NULL AS ROWNUM,NULL AS S1,NULL AS S2,NULL AS S3,NULL AS S4,  " ||
                "    NULL AS S5,NULL AS S6,NULL AS JYFY,NULL AS CCCB_ZC,NULL AS CCCB_ZR  ,  " || I_RQ || " as cjrq " ||
                " from " || F_IDS_GET_TABLENAME('sparkZsbgResult', I_KHH) ||" t, " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" s  " ||
                " where t.tzzh_id = s.tzzh_id and t.khh = s.khh and t.jys = s.jys and t.mjdm = s.zqdm";
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_JGMXLS_HIS_QS", I_RQ, I_KHH);
  END;

------------------        //11清仓数据处理-------------------------
--------------------------- //成本盈亏---------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkTzsy', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkTzsy', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkTzsy', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkTzsy', I_KHH);
    l_sqlBuf :=
                "SELECT D.KCRQ,  " ||
                "       D.TZZH_ID,  " ||
                "       D.KHH,  " ||
                "       D.GDH,  " ||
                "       D.JYS,  " ||
                "       D.ZQDM,  " ||
                "       D.ZQLB,  " ||
                "       D.BZ,  " ||
                "       D.LJMRJE AS MRJE,  " ||
                "       D.LJMCJE AS MCJE,  " ||
                "       D.LJJYFY AS JYFY,  " ||
                "       CASE  " ||
                "         WHEN IS_ETFHBSH = 1 THEN  " ||
                "          D.LJMCJE - D.LJMRJE  " ||
                "         ELSE  " ||
                "          NVL(D.LJYK_TZ, D.LJYK)  " ||
                "       END AS LJYK,  " ||
                "       CASE  " ||
                "         WHEN RQ = KCRQ AND IS_ETFHBSH = 1 THEN " ||  -- 如果当日买入当日卖出，则当日盈亏和累计盈亏一致
                "          D.LJMCJE - D.LJMRJE  " ||
                "         ELSE  " ||
                "          D.ZQSZ - D.SRZQSZ -  " ||
                "          (NVL(D.DRMRJE_TZ, D.DRMRJE) - NVL(D.DRMCJE_TZ, D.DRMCJE))  " ||
                "       END AS DRYK,  " ||
                "     D.QCRQ  " ||
                "  FROM (SELECT " || I_RQ  || " as RQ,  " ||
                "               T.KHH,  " ||
                "               T.TZZH_ID,  " ||
                "               T.GDH,  " ||
                "               T.BZ,  " ||
                "               T.JYS,  " ||
                "               T.ZQDM,  " ||
                "               T.ZQLB,  " ||
                "               T.KCRQ,  " ||
                "               T.ZQSL,  " ||
                "               T.DRMCSL,  " ||
                "               T.LJMRJE - NVL(T.LJSGJE_SJ_ETFHB, 0) +  " ||
                "               NVL(T.LJSGJE_SF_ETFHB, 0) AS LJMRJE,  " ||
                "               T.LJMCJE - NVL(T.LJSHJE_SJ_ETFHB, 0) +  " ||
                "               NVL(T.LJSHJE_SS_ETFHB, 0) AS LJMCJE,  " ||
                "               T.LJJYFY,  " ||
                "               T.CCCB,  " ||
                "               T.LJYK,  " ||
                "               T.CCCB_TZ,  " ||
                "               T.LJYK_TZ,  " ||
                "               T.ZQSZ,  " ||
                "               T.SRZQSL,  " ||
                "               T.SRZQSZ,  " ||
                "               T.DRMRJE,  " ||
                "               T.DRMCJE,  " ||
                "               T.DRSHDQSL,  " ||
                "               T.DRSHDQJE,  " ||
                "               T.DRSHSYSL,  " ||
                "               T.SRSHSYSL,  " ||
                "               (T.DRMRJE - NVL(T.DRHBCFJE_ZR, 0) +  " ||
                "               NVL(T.DRHBCFJE_ZR_TZ, 0) - NVL(T.YK_ZR, 0)) AS DRMRJE_TZ,  " ||  -- 基金合并拆分时候盈亏有带过来，所以在当日盈亏里面也要体现出来，这里得扣减掉
                "               CASE  " ||
                "                 WHEN (DRSHDQSL > 0 AND SRZQSL > 0 AND DRSHDQSL <> SRSHSYSL) THEN  " ||
                -- 这个条件是为了过滤正常调度处理，赎回操作T+1日就已经算清仓处理的数据情况
                -- 基金合并拆分时候盈亏有带过来，所以在当日盈亏里面也要体现出来，这里得扣减掉；同时补充上赎回到期金额
                "                  (T.DRMCJE - NVL(T.DRHBCFJE_ZC, 0) +  " ||
                "                  NVL(T.DRHBCFJE_ZC_TZ, 0) - NVL(T.YK_ZC, 0) +  " ||
                "                  NVL(DRSHDQJE, 0))  " ||
                "                 ELSE  " ||
                -- 基金合并拆分时候盈亏有带过来，所以在当日盈亏里面也要体现出来，这里得扣减掉
                "                  (T.DRMCJE - NVL(T.DRHBCFJE_ZC, 0) +  " ||
                "                  NVL(T.DRHBCFJE_ZC_TZ, 0) - NVL(T.YK_ZC, 0))  " ||
                "               END AS DRMCJE_TZ,  " ||
                "               T.QCRQ,  " ||
                "               CASE  " ||
                "                 WHEN NVL(T.LJSHJE_SS_ETFHB, 0) <> 0 OR  " ||
                "                      NVL(T.LJSGJE_SF_ETFHB, 0) <> 0 THEN  " ||
                "                  1  " ||
                "                 ELSE  " ||
                "                  0  " ||
                "               END AS IS_ETFHBSH  " ||
                "          FROM " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" T  " ||
                "         WHERE T.QCRQ = " || I_RQ  || ") D";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;


------------------------获取字段----------------------------------------------
     l_tableArr := get_columns(l_tableName);
      l_columns := '( ';
      FOR indx IN l_tableArr.first() .. l_tableArr.last() LOOP
        IF indx = l_tableArr.last() THEN
          l_columns := l_columns || l_tableArr(indx) || ') ';
        ELSE
          l_columns := l_columns || l_tableArr(indx) || ',';
        END IF;
      END LOOP;


    -------------------------------  //待交收---------------------------------
  BEGIN
    l_sqlBuf :="SELECT " ||
                "         CJRQ AS KCRQ,  " ||
                "         0 AS TZZH_ID,  " ||
                "         KHH,  " ||
                "         GDH,  " ||
                "         JYS,  " ||
                "         ZQDM,  " ||
                "         ZQLB,  " ||
                "         BZ,  " ||
                "         CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE_2) ELSE ABS(YSJE) END) AS DECIMAL(18,2)) AS MRJE,  " ||
                "         CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE) ELSE CJJE + LXJE END) AS DECIMAL(18,2)) AS MCJE,  " ||
                "         0 AS JYFY,  " ||
                "         CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE)-ABS(YSJE_2) ELSE LXJE-(ABS(YSJE)-CJJE) END) AS DECIMAL(18,2)) AS LJYK,  " ||
                "         CAST(SUM(CASE WHEN JYLB='4' THEN ABS(YSJE)-ABS(YSJE_2) ELSE LXJE-(ABS(YSJE)-CJJE) END) AS DECIMAL(18,2)) AS DRYK,  " ||
                I_RQ || " AS QCRQ  " ||
                "    FROM DSC_BAS.T_DJSQSZL_HIS T  " ||
                "   WHERE ZQLB IN ('H0', 'H1', 'H3', 'H4')" ||
                "     AND SETTLE_DATE_2 <= " || I_RQ ||
                "     AND SETTLE_DATE_2 > " || l_lastDay ||
                "     AND RQ = " || l_lastDay ||
                "   GROUP BY SETTLE_DATE_2, CJRQ, KHH, GDH, JYS, ZQDM, ZQLB, BZ";

    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkTzsy', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;
  END;

 ---------------------      //去除无用数据---------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkTzsyDel', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkTzsyDel', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkTzsyDel', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkTzsyDel', I_KHH);
    l_sqlBuf :=
                "SELECT T.kcrq,  " ||
                "       T.tzzh_id,  " ||
                "       T.khh,  " ||
                "       T.gdh,  " ||
                "       T.jys,  " ||
                "       T.zqdm,  " ||
                "       T.ZQLB,  " ||
                "       T.bz,  " ||
                "       T.mrje,  " ||
                "       T.mcje,  " ||
                "       T.jyfy,  " ||
                "       T.ljyk,  " ||
                "       T.dryk,  " ||
                "       T.qcrq  " ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkTzsy', I_KHH) ||" T  " ||
                " WHERE ((DRYK = 0 AND LJYK = 0 AND ZQLB IN ('L0')) AND EXISTS  " ||
                "        (SELECT 1  " ||
                "           FROM " || F_IDS_GET_TABLENAME('sparkCbjsResultNew', I_KHH) ||" D  " ||
                "          WHERE (D.DRHBCFSL_ZC > 0 OR D.DRHBCFSL_ZR > 0)  " ||
                "            AND D.JYS = T.JYS  " ||
                "            AND D.ZQDM = T.ZQDM  " ||
                "            AND D.KHH = T.KHH))";

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkTzsyResult', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkTzsyResult', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkTzsyResult', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkTzsyResult', I_KHH);
    l_sqlBuf :=
                " SELECT " ||
                "         KCRQ,  " ||
                "         TZZH_ID,  " ||
                "         KHH,  " ||
                "         GDH,  " ||
                "         T.JYS,  " ||
                "         T.ZQDM,  " ||
                "         NVL(Z.ZQLB, T.ZQLB) AS ZQLB,  " ||
                "         T.BZ,  " ||
                "         CAST(ROUND(MRJE, 2) AS DECIMAL(16, 2)) AS MRJE,  " ||
                "         CAST(ROUND(MCJE, 2) AS DECIMAL(16, 2)) AS MCJE,  " ||
                "         CAST(ROUND(JYFY, 2) AS DECIMAL(16, 2)) AS JYFY,  " ||
                "         CAST(ROUND(LJYK, 2) AS DECIMAL(16, 2)) AS LJYK,  " ||
                "         CAST(ROUND(DRYK, 2) AS DECIMAL(16, 2)) AS DRYK, " ||
                I_RQ || " as qcrq" ||
                "    FROM " || F_IDS_GET_TABLENAME('sparkTzsy', I_KHH) ||" T  " ||
                "    LEFT JOIN CUST.T_ZQDM Z  " ||
                "      ON (T.JYS = Z.JYS AND T.ZQDM = Z.ZQDM)" ||
                "    WHERE NOT EXISTS (SELECT 1 FROM " || F_IDS_GET_TABLENAME('sparkTzsyDel', I_KHH) ||
                "  D WHERE  T.KHH = D.KHH AND T.GDH = D.GDH AND T.JYS = D.JYS AND T.ZQDM = D.ZQDM AND T.TZZH_ID = D.TZZH_ID)" ;

                  -- left anti join不支持，改为not exists
                /*LEFT ANTI  JOIN
                  D
                  ON (T.KHH = D.KHH AND T.GDH = D.GDH AND T.JYS = D.JYS AND
                     T.ZQDM = D.ZQDM AND T.TZZH_ID = D.TZZH_ID)  */

    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;


  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionCloumn, partitionValue, khh)
     */
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_TZSY", I_RQ, I_KHH);
  END;
end;

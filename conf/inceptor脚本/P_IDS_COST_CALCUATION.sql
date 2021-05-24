create or replace procedure cust.p_ids_cost_calcuation(
  --输入变量
  I_RQ IN INT,
  I_KHH IN STRING

) is

/******************************************************************
  *文件名称：CUST.P_IDS_COST_CALCUATION
  *项目名称：IDS计算
  *文件说明：集中交易-成本盈亏修正处理
 
  创建人： 胡阳明
  功能说明：集中交易-成本盈亏修正处理

  参数说明

  修改者        版本号        修改日期        说明
  胡阳明        v1.0.0        2019/6/24       创建
  燕居庆        v1.0.1        2019/8/15       根据最新代码调整成本计算
  燕居庆        v1.0.2        2019/9/13       对标java-ids的4858版本：
                                              1.ETF货币基金的申购实际付出金额修正，DRJYFY等字段默认值 NULL -> 0 
                                              2.当日买入卖出相关数据，SRSHSYSL等字段默认值 NULL -> 0
                                              3.去除 处理上日赎回顺延数量 的处理
                                              4.市价业务修正调整
                                              5.调整开仓日期与清仓日期
  燕居庆        v1.0.3        2019/9/25      对标java-ids的5051版本：
                                               1.生成当日买入、当日卖出相关数据 增加cjrq<=ksrq
                                               2.增加基金
  燕居庆        v1.0.4        2019/9/27       对标java-ids 5050版本：
                                               1.生成当日买入卖出调整
  燕居庆        v1.0.5        2019/10/29      对标java-ids 5297版本：
                                               1.增加深圳LOF赎回顺延
  燕居庆        v1.0.6        2019/11/08      对标java-ids 5435版本： 
                                               1.增加交易类别 78盘后买入、79盘后卖出的处理
  燕居庆        v1.0.7        2019/11/18      修改：增加ETF认购处理
  燕居庆        v1.0.8        2019/11/29      修改：增加科创板主题基金L0类别的认购处理
  燕居庆        v1.0.9        2020/05/07      修改：针对深圳配股上市，计算成本价为数量*配股价
  邱建锋		v1.1.3		  2020/07/24	  修改：1、基金赎回后成本未降低，调整处理逻辑
                                                    2、剔除送股计入买入金额
  邱建锋		v1.1.4		  2020/07/27      修改：'CD','AD','BD' 退市转出不算清仓金额
  邱建锋		v1.1.5		  2020/07/28      修改：调整新三板精选层新股申购持仓成本
  燕居庆        v1.1.6        2020/09/14      修改：基金份额折算转入不计算买入
  钟梦涵        v1.1.7        2021/01/25      修改：调整行情表中港股zxj*汇率
*******************************************************************/
l_sqlBuf STRING; --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_sqlWhereLastDay2 STRING;
l_lastDay INT;
l_lastDay2 INT;
l_Day1 INT;--T+1
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL;
l_columns STRING;
l_sql STRING;
BEGIN
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  --取T-2
  SELECT F_GET_JYR_DATE(I_RQ, -2) INTO l_lastDay2 FROM system.dual;
 ---取T+1
  SELECT F_GET_JYR_DATE(I_RQ, 1) INTO l_Day1 FROM system.dual; 

  IF I_KHH IS NULL THEN
    l_sqlWhereLastDay := l_lastDay;
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay2:= l_lastDay2;
  ELSE 
    l_sqlWhereLastDay := l_lastDay || ' and khh = ' || I_KHH;
    l_sqlWhereCurrentDay := I_RQ || ' and khh = ' || I_KHH;
    l_sqlWhereLastDay2:=l_lastDay2 || ' and khh = ' || I_KHH;   
  END IF;
 ----------------------------------      //基金净值------------------------------------- 
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJjjzTpre1', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJjjzTpre1', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJjjzTpre1', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkJjjzTpre1', I_KHH);
    l_sqlBuf := 'select * from DSC_BAS.T_FJJJJZ_HIS D where jzrq = ' || l_lastDay;--  T-1日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
  END;
  
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJjjzTpre2', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJjjzTpre2', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJjjzTpre2', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkJjjzTpre2', I_KHH);
    l_sqlBuf := 'select * from DSC_BAS.T_FJJJJZ_HIS where  jzrq=' || l_lastDay2;--  T-2日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;  
  
 ---------------------------------- //成本计算----------------------------------------------- 
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkCbjsPre1', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkCbjsPre1', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkCbjsPre1', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkCbjsPre1', I_KHH);
    l_sqlBuf := 'select * from cust.T_ZQYE_CBJS where rq = ' || l_sqlWhereLastDay;--  T-1日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  
  
 ----------------------------------   //持仓------------------------------------ 
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZqyeSR', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZqyeSR', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH);
    l_sqlBuf := 'select * from cust.t_zqye_his where rq = ' || l_sqlWhereLastDay;--------T-1
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkZqyeDR', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkZqyeDR', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkZqyeDR', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkzqyeDR', I_KHH);
    l_sqlBuf := "select 0 tzzh_id,rq,khh,jys,gdh,zqdm,zqlb,bz,zqsl,null as kcrq,null as fltsl,zxsz,"  ||
                " null as cccb,null as cbj,null as ljyk,null as tbcccb,null as tbcbj,null as dryk, ZQSL AS ZQSL_ORIG"  || 
                " from dsc_bas.t_zqye_his t where zqdm not in  ('888880','900000') and t.zqlb <> 'F8' and t.zqsl>0 AND T.ZQDM NOT LIKE 'SHRQ%' AND T.ZQDM NOT LIKE 'SZRQ%'  and rq = " || l_sqlWhereCurrentDay;-----T日
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  
--------------------------------------        //待交收----------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkDjsqszl', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkDjsqszl', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkDjsqszl', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkDjsqszl', I_KHH);
    l_sqlBuf := 'select * from cust.T_DJSQSZL where SETTLE_DATE_2 = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
----------------------------------    //交割清算,前面已经读取-----------------------------------------
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJgmxlsQS', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJgmxlsQS', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH);
    l_sqlBuf := 'select * from cust.t_jgmxls_his_qs where cjrq = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    /**
     * 临时表名
     * 由于集中/两融/期权可能存在相同临时表，因此在改造时，需要添加相关前缀：
     * 集中：F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH);
     * 两融：F_IDS_GET_TABLENAME('xy_sparkJgmxlsHis', I_KHH);
     * 期权：F_IDS_GET_TABLENAME('so_sparkJgmxlsHis', I_KHH);
     */
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH);
    l_sqlBuf := 'select * from cust.t_jgmxls_his where cjrq = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

--------------------------------------    //1、获取货币基金申购赎回--------------------------------------  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkHbjjsgsh', I_KHH);
    l_sqlBuf := "SELECT T.TZZH_ID," ||
                "         T.KHH," ||
                "         T.GDH," ||
                "         T.JYS," ||
                "         T.ZQDM," ||
                "         T.SGSHDM," ||
                "         NULL AS ZQLB," ||
                "         T.BZ," ||
                "         CAST(T.DRSGJE AS DECIMAL(16, 2)) AS DRSGJE," ||
                "         JSRQ AS DZRQ_SG," ||
                "         CAST(T.DRSGSL AS INT) AS DRSGSL," ||
                "         JSRQ AS WTRQ_SH," ||
                "         CAST(T.DRSHSL AS INT) AS DRSHSL," ||
                "         CAST(T.DRSHJE AS DECIMAL(16, 2)) AS DRSHJE" ||
                "    FROM (SELECT JG.TZZH_ID," ||
                "                 JG.KHH," ||
                "                 JG.GDH," ||
                "                 JG.JYS," ||
                "                 D.JJDM AS ZQDM," ||
                "                 D.SGSHDM," ||
                "                 JG.BZ," ||
                "                 CASE WHEN JYLB='29' AND YSJE<0 AND CJRQ<JSRQ THEN JSRQ ELSE CJRQ END AS JSRQ," ||
                "                 SUM(CASE" ||
                "                       WHEN (JYLB = '30' AND YSJE > 0) OR (JYLB = '29' AND YSJE > 0) THEN" ||
                "                        ABS(YSJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHJE," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '30' AND YSSL < 0 AND SEQNO!=0 THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHSL," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '29' AND YSJE < 0 THEN" ||
                "                        ABS(YSJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSGJE," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '29' AND YSSL > 0 THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSGSL" ||
                "            FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH)  ||" JG, CUST.T_HBJJXX D" ||
                "           WHERE JG.JYS = D.JYS" ||
                "             AND (JG.ZQDM = D.JJDM OR JG.ZQDM = D.SGSHDM)" ||
                "             AND JG.JYLB IN ('29', '30')" ||
                "             AND NOT (ZQLB IN ('E0','E4') AND JYLB = '29' AND SEQNO = 0)" || // 对ETF申购交割单进行去重处理
                "             AND D.JYS = '2'" ||  // 先修复上海的
                "           GROUP BY JG.TZZH_ID," ||
                "                    JG.KHH," ||
                "                    JG.GDH," ||
                "                    JG.JYS," ||
                "                    D.JJDM," ||
                "                    D.SGSHDM," ||
                "                    JG.BZ," ||
                "                    CASE WHEN JYLB = '29' AND YSJE < 0 AND CJRQ<JSRQ THEN JSRQ ELSE CJRQ END) T";
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
----------------------   //更新到表--------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkHbjjsgshResult', I_KHH);
    l_sqlBuf := "  SELECT T.TZZH_ID," ||
                "         T.KHH," ||
                "         T.GDH," ||
                "         T.JYS," ||
                "         T.ZQDM," ||
                "         T.SGSHDM," ||
                "         ZQLB," ||
                "         T.BZ," ||
                "         DRSGJE," ||
                "         DZRQ_SG," ||
                "         DRSGSL," ||
                "         WTRQ_SH," ||
                "         DRSHSL," ||
                "         DRSHJE," ||
                I_RQ || " AS RQ" ||
                "    FROM " || F_IDS_GET_TABLENAME('sparkHbjjsgsh', I_KHH) || " T";    
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionValue, khh)
     */ 
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_ZQYE_CBJS_HBJJSGSH",  I_RQ, I_KHH);
  END; 
  
  ----------------------------  //6、ETF货币基金的申购实际付出金额修正 -------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkHBJJSGSHsg', I_KHH);
    l_sqlBuf := "select * from CUST.T_ZQYE_CBJS_HBJJSGSH where DZRQ_SG = " || l_sqlWhereCurrentDay;
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkHBJJSGSHsh', I_KHH);
    l_sqlBuf := "select * from CUST.T_ZQYE_CBJS_HBJJSGSH where WTRQ_SH = " || l_sqlWhereCurrentDay;
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH);
    --l_tableName := F_IDS_GET_TABLENAME('sparkHBJJSGSHsgRe', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID," ||
                "         DZRQ_SG AS RQ," ||
                "         KHH," ||
                "         JYS," ||
                "         GDH," ||
                "         ZQDM," ||
                "         BZ," ||
                "         SUM(T.DRSGSL) AS DRYSSL," ||
                "         SUM(T.DRSGJE) AS DRMRJE," ||
                "         SUM(T.DRSGSL) AS DRMRSL," ||
                "         0 AS DRMCJE," ||
                "         0 AS DRMCSL," ||
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
                "         SUM(T.DRSGSL) AS DRSGSL_ETFHB," ||
                "         0 AS DRSHJE_SJ_ETFHB," ||
                "         SUM(T.DRSGJE) AS DRSGJE_SJ_ETFHB," ||
                "         0 AS DRPGJE," ||
                "         0 AS DRPGSL," ||
                "         0 AS drshje_ss_etfhb," ||
                "         SUM(T.DRSGJE) AS DRSGJE_SF_ETFHB" ||
                "    FROM "||  F_IDS_GET_TABLENAME('sparkHBJJSGSHsg', I_KHH) ||"  T" ||
                "   GROUP BY TZZH_ID, DZRQ_SG, KHH, JYS, GDH, ZQDM, BZ" ||
                " UNION ALL " ||
                " SELECT  TZZH_ID," ||
                "         WTRQ_SH AS RQ," ||
                "         KHH," ||
                "         JYS," ||
                "         GDH," ||
                "         ZQDM," ||
                "         BZ," ||
                "         -SUM(T.DRSHSL) AS DRYSSL," ||
                "         0 AS DRMRJE," ||
                "         0 AS DRMRSL," ||
                "         SUM(DRSHJE) AS DRMCJE," ||
                "         SUM(DRSHSL) AS DRMCSL," ||
                "         0 AS DRJYFY," ||
                "         0 AS DRSHDQSL," ||
                "         0 AS DRSHDQJE," ||
                "         0 AS DRSHSYSL," ||
                "         0 AS SRSHSYSL," ||
                "         0 AS DRHBCFJE_ZR," ||
                "         0 AS DRHBCFSL_ZR," ||
                "         0 AS DRHBCFJE_ZC," ||
                "         0 AS DRHBCFSL_ZC," ||
                "         SUM(T.DRSHSL) AS DRSHSL_ETFHB," ||
                "         0 AS DRSGSL_ETFHB," ||
                "         SUM(T.DRSHJE) AS DRSHJE_SJ_ETFHB," ||
                "         0 AS DRSGJE_SJ_ETFHB," ||
                "         0 AS DRPGJE," ||
                "         0 AS DRPGSL," ||
                "         SUM(T.DRSHJE) AS drshje_ss_etfhb," ||
                "         0 AS DRSGJE_SF_ETFHB" ||
                "   FROM " || F_IDS_GET_TABLENAME('sparkHBJJSGSHsh', I_KHH) ||" T" ||
                "  GROUP BY TZZH_ID, WTRQ_SH, KHH, JYS, GDH, ZQDM, BZ";
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  BEGIN
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
   END;
 -----------------------------        //2、生成当日买入、当日卖出相关数据----------------------------------- 
  BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH);
    l_sqlBuf := "SELECT A.TZZH_ID," || I_RQ  ||
                "         AS RQ," ||
                "         A.KHH," ||
                "         A.JYS," ||
                "         A.GDH," ||
                "         A.ZQDM," ||
                "         A.BZ," ||
                "         CAST(A.YSSL AS DECIMAL(22,2)) AS DRYSSL," ||
                "         CAST(A.MRJE AS DECIMAL(16,2)) AS DRMRJE," ||
                "         CAST(A.MRSL AS DECIMAL(22,2)) AS DRMRSL," ||
                "         CAST(A.MCJE AS DECIMAL(16,2)) AS DRMCJE," ||
                "         CAST(A.MCSL AS DECIMAL(22,2)) AS DRMCSL," ||
                "         CAST(A.JYFY AS DECIMAL(16,2)) AS DRJYFY," ||
                "         CAST(A.DRSHDQSL AS DECIMAL(16,2)) AS DRSHDQSL," ||
                "         CAST(A.DRSHDQJE AS DECIMAL(16,2)) AS DRSHDQJE," ||
                "         CAST(A.DRSHSYSL AS DECIMAL(16,2)) AS DRSHSYSL," ||
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
                "         0 AS DRSHJE_SS_ETFHB," ||
                "         NULL AS DRSGJE_SF_ETFHB" ||
                "    FROM (SELECT TZZH_ID," ||
                "                 KHH," ||
                "                 GDH," ||
                "                 JYS," ||
                "                 ZQDM," ||
                "                 BZ," ||
                "                 SUM(YSSL) AS YSSL," ||  -- 1买入,16送股,61融资买入,41认购,42申购,59大宗买入,60大宗卖出,80申保,83中签 11转债转股买入，78盘后买入
                "                 SUM(CASE" ||
                "                       WHEN JYLB IN ('1', '59', '61', '41', '83', '93', '94','3','78','32') OR" ||
                "                            (JYLB = '80' AND NOT (JYS = '2' AND ZQLB='L0')) OR" ||  --20191129科创主题基金处理
                "                            (JYLB = '41' AND NOT (JYS = '2' AND ZQLB='L3' AND SEQNO=0)) OR" ||  --20191129科创主题基金处理
                "                            (JYLB = '49' AND YSJE<0) OR (JYLB = '42' AND JSRQ <> 0) OR (JYLB='11' AND YSSL>0) THEN" ||     --(JYLB = '42' AND JSRQ <> 0)
                "                        ABS(YSJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MRJE," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB IN" ||
                "                            ('1', '16', '59', '61', '41',  '83','3','78','32') OR" ||
                "                            (JYLB = '80' AND NOT (JYS = '2' AND ZQLB='L0')) OR" ||
                "                            (JYLB = '41' AND NOT (JYS = '2' AND ZQLB='L3' AND SEQNO=0)) OR" ||  --20191129科创主题基金处理
                "                            (JYLB = '42' AND JSRQ <> 0) OR (JYLB='11' AND YSSL>0) THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MRSL," || -- 2卖出,6分红,17兑付,22兑息,43赎回,45分红,71融资强平,81退保 11转债转股卖出,79盘后卖出
                "                 SUM(CASE" ||
                "                       WHEN ZQLB = 'L0' and JYLB = '81' THEN" ||
                "                        0" ||
                "                       WHEN JYLB IN" ||
                "                            ('2', '6', '17', '22', '45', '60', '62', '71', '81', '76','79') OR" ||
                "                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '2') OR (JYLB='11' AND YSSL<0) OR " ||  
                "                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '1') OR " ||   --20200724
                "                            (JYLB = '43' AND JYS = '2' AND JSRQ <> 0 AND CJRQ >= JSRQ AND ZQLB='J0') OR (JYLB='49' AND YSJE>0) THEN" ||
                "                        ABS(YSJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MCJE," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '43' AND ABS(YSJE) = 0 THEN" ||
                "                        0" ||
                "                       WHEN JYLB IN" ||
                "                            ('2', '6', '17', '22', '45', '60', '62', '71', '81', '76','79') OR" ||
                "                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '2') OR (JYLB='11' AND YSSL<0) OR " ||
                "                            (JYLB = '43' AND JSRQ <> 0 AND CJRQ <= JSRQ AND JYS = '1') OR " ||
                "                            (JYLB = '43' AND JYS = '2' AND JSRQ <> 0 AND CJRQ >= JSRQ AND ZQLB='J0') THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MCSL," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '43' AND CJRQ > JSRQ AND JSRQ <> 0 THEN" || -- 实际赎回到期那天不算费用，因为已经在交收顺延T+1计算了
                "                        0" ||
                "                       ELSE" ||
                "                        S1 + S2 + S3 + S4 + S5 + S6" ||
                "                     END) AS JYFY," ||  -- 赎回到期特殊处理
                "                 SUM(CASE" ||
                "                       WHEN (JYLB = '43' AND CJRQ > JSRQ AND JSRQ <> 0) THEN" ||
                "                        ABS(YSJE)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHDQJE," ||
                "                 SUM(CASE" ||
                "                       WHEN (JYLB = '43' AND CJRQ > JSRQ AND JSRQ <> 0) THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHDQSL," ||
                "                 SUM(CASE" ||
                "                       WHEN (JYLB = '43' AND CJRQ <= JSRQ) THEN" ||
                "                        ABS(YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHSYSL" ||
                "            FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH) || 
                "           WHERE (YSSL <> 0 OR YSJE <> 0)" ||
                "           GROUP BY TZZH_ID, KHH, GDH, JYS, ZQDM, BZ) A";    
                  
                  
    -- 数据插入至资产统计临时表sparkZqyeCbjs_0
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH) || l_columns || l_sqlBuf;
    EXECUTE IMMEDIATE l_sql;  

  END;
  
  
      
  
---------------------------------------//4、视作按市价的业务:指定撤指(9,10),回售(12),赎回(13),转托管(7,15),ETF申赎(29,30),基金拆分合并(47,48)，非交易过户(31),转债转股(11)(11改为盘中交易)-
  BEGIN
    --l_tableName := F_IDS_GET_TABLENAME('sparkGpzyResult', I_KHH);
    l_sqlBuf :="SELECT A.TZZH_ID," || I_RQ ||
                "         AS RQ," ||
                "         A.KHH," ||
                "         A.JYS," ||
                "         A.GDH," ||
                "         A.ZQDM," ||
                "         A.BZ," ||
                "         0 AS DRYSSL," ||
                "         CAST(A.MRJE AS DECIMAL(16,2)) AS DRMRJE," ||
                "         CAST(A.MRSL AS DECIMAL(22,2)) AS DRMRSL," ||
                "         CAST(A.MCJE AS DECIMAL(16,2)) AS DRMCJE," ||
                "         CAST(A.MCSL AS DECIMAL(22,2)) AS DRMCSL," ||
                "         CAST(A.JYFY AS DECIMAL(16,2)) AS DRJYFY," ||
                "         0 AS DRSHDQSL," ||
                "         0 AS DRSHDQJE," ||
                "         0 AS DRSHSYSL," ||
                "         0 AS SRSHSYSL," ||
                "         CAST(A.DRHBCFJE_ZR AS DECIMAL(16,2)) AS DRHBCFJE_ZR," ||
                "         CAST(A.DRHBCFSL_ZR AS DECIMAL(16,2)) AS DRHBCFSL_ZR," ||
                "         CAST(A.DRHBCFJE_ZC AS DECIMAL(16,2)) AS DRHBCFJE_ZC," ||
                "         CAST(A.DRHBCFSL_ZC AS DECIMAL(16,2)) AS DRHBCFSL_ZC," ||
                "         CAST(A.DRSHSL_ETFHB AS DECIMAL(16,2)) AS DRSHSL_ETFHB," ||
                "         CAST(A.DRSGSL_ETFHB AS DECIMAL(16,2)) AS DRSGSL_ETFHB," ||
                "         CAST(A.DRSHJE_SJ_ETFHB AS DECIMAL(16,2)) AS DRSHJE_SJ_ETFHB," ||
                "         CAST(A.DRSGJE_SJ_ETFHB AS DECIMAL(16,2)) AS DRSGJE_SJ_ETFHB," ||
                "         0 AS DRPGJE," ||
                "         0 AS DRPGSL," ||
                "         CAST(A.DRSHJE_SS_ETFHB AS DECIMAL(16,2)) AS DRSHJE_SS_ETFHB," ||
                "         CAST(A.DRSGJE_SF_ETFHB AS DECIMAL(16,2)) AS DRSGJE_SF_ETFHB"  ||
                "    FROM (SELECT B.TZZH_ID," ||
                "                 B.KHH," ||
                "                 B.GDH," ||
                "                 B.JYS," ||
                "                 B.ZQDM," ||
                "                 B.BZ," ||
                "                 ROUND(SUM(CASE" ||
				"							  WHEN JYLB='18' AND YSJE<0 AND CJBH LIKE '%内部代码变更%' THEN ABS(YSJE)     " || -- 20200528 证券代码变更修复,位置不能下移。
				"							  WHEN JYLB='18' AND IS_XSB_JXC=1 THEN ABS(B.YSSL * B.XSB_FXJ) "||   -- 20200728 新三板精选层成本调整
                "                             WHEN B.JYLB='18' AND B.IS_PG=1 THEN B.CJSL*B.PGJG" ||
                "                             WHEN B.JYLB='18' AND B.JYS='2' AND B.IS_PG=1 AND B.CJBH='上市流通' THEN B.CJSL*B.PGJG" ||
                "                             WHEN B.JYLB='18' AND B.JYS='1' AND B.IS_PG=1 AND B.CJBH='上市转入' THEN B.CJSL*B.PGJG" ||
                "                             WHEN B.JYLB='18' AND B.JYS='2' AND B.IS_KG=1 AND B.CJBH='上市流通' THEN 0" ||
                "                             WHEN B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '18' AND B.CJBH = '份额变动' THEN 0 " ||  --20191129科创主题基金处理
                "                             WHEN B.JYLB='18' AND B.ZQLB='E0' AND B.CJBH='调整变动' THEN 0 " ||  --20200914基金份额折算转入不计算买入
                "                             WHEN (JYLB IN ('15', '18', '47', '48') OR (JYLB IN ('29','30') AND IS_ETFHB=0)) AND YSSL > 0 THEN" ||
                "                              ABS(B.YSSL * A.ZXJ)" ||
                "                             WHEN JYLB IN ('9','20','38') OR (JYLB IN ('31','65') AND YSSL>0) THEN ABS(B.CJJE)" ||
                "                             ELSE" ||
                "                              0" ||
                "                           END)," ||
                "                       2) AS MRJE," ||
                "                 SUM(CASE" ||
                "                       WHEN B.JYLB='18' AND B.ZQLB='E0' AND B.CJBH='调整变动' THEN 0 " || --20200914基金份额折算转入不计算买入
                "                       WHEN ((JYLB IN" ||
                "                          ('15', '18','47', '48')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) AND" ||
                "                            YSSL > 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       WHEN JYLB IN ('9','20','38') OR (JYLB IN ('31','65') AND YSSL>0) THEN ABS(B.CJSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MRSL," ||
                "                 ROUND(SUM(CASE" ||
				"                             WHEN JYLB='19' AND YSJE>0 AND CJBH LIKE '%内部代码变更' THEN ABS(YSJE) "||
                "                             WHEN (JYLB IN ('13', '47', '48') OR (JYLB = '19' AND ZQLB NOT IN ('A4', 'C4','AD','BD')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) " ||  -- 20200727 'CD','AD','BD' 退市转出不算清仓金额
                "                                   AND NOT (B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '19' AND B.CJBH = '份额变动') " ||
                "                                   AND NOT (JYLB='19' AND B.ZQLB='Z4' AND B.CJBH='调整变动')" ||
                "                                   AND NOT (JYLB='19' AND B.ZQLB='Z6' AND B.JYS='2' AND (B.CJBH='份额变动' OR B.CJBH='no cjbh')) " ||
                "                                   AND YSSL < 0 THEN" ||
                "                              ABS(B.YSSL * A.ZXJ)" ||
                "                             WHEN JYLB IN ('7','10','21','37') OR (JYLB IN ('12','31','66') AND YSSL<0) THEN ABS(B.CJJE)" ||
                "                             WHEN JYLB IN ('31') AND B.CJJE=0 AND B.YSSL=0 AND B.YSJE>0 THEN B.YSJE" ||
                "                             ELSE" ||
                "                              0" ||
                "                           END)," ||
                "                       2) AS MCJE," ||
                "                 SUM(CASE" ||
                "                       WHEN (JYLB IN ('13', '47', '48') OR (JYLB = '19' AND ZQLB NOT IN ('A4', 'C4')) or (JYLB IN ('29','30') AND IS_ETFHB=0)) " ||
                "                            AND NOT (B.ZQLB = 'L0' AND B.JYS = '2' AND B.JYLB = '19' AND B.CJBH = '份额变动') " ||
                "                            AND NOT (JYLB='19' AND B.ZQLB='Z6' AND B.JYS='2' AND (B.CJBH='份额变动' OR B.CJBH='no cjbh'))" ||
                "                            AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       WHEN JYLB IN ('7','10','21','37') OR (JYLB IN ('12','31','66') AND YSSL<0) THEN ABS(B.CJSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS MCSL," ||
                "                 SUM(S1 + S2 + S3 + S4 + S5 + S6) AS JYFY," ||
                "                 ROUND(SUM(CASE" ||
                "                             WHEN ((JYLB IN ('18') AND CJBH = '拆分合并') OR" ||
                "                                  JYLB IN ('47', '48')) AND YSSL > 0 THEN" ||
                "                              ABS(B.YSSL * CASE" ||
                "                                    WHEN A.ZXJ > 0 THEN" ||
                "                                     A.ZXJ" ||
                "                                    ELSE" ||
                "                                     A.ZSP" ||
                "                                  END)" ||
                "                             ELSE" ||
                "                              0" ||
                "                           END)," ||
                "                       2) AS DRHBCFJE_ZR," || -- 基金合并拆分数据
                "                 SUM(CASE" ||
                "                       WHEN ((JYLB IN ('18') AND CJBH = '拆分合并') OR" ||
                "                            JYLB IN ('47', '48')) AND YSSL > 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRHBCFSL_ZR," ||
                "                 ROUND(SUM(CASE" ||
                "                             WHEN ((JYLB IN ('19') AND CJBH = '拆分合并') OR" ||
                "                                  JYLB IN ('47', '48')) AND YSSL < 0 THEN" ||
                "                              ABS(B.YSSL * CASE" ||
                "                                    WHEN A.ZXJ > 0 THEN" ||
                "                                     A.ZXJ" ||
                "                                    ELSE" ||
                "                                     A.ZSP" ||
                "                                  END)" ||
                "                             ELSE" ||
                "                              0" ||
                "                           END)," ||
                "                       2) AS DRHBCFJE_ZC," ||
                "                 SUM(CASE" ||
                "                       WHEN ((JYLB IN ('19') AND CJBH = '拆分合并') OR" ||
                "                            JYLB IN ('47', '48')) AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRHBCFSL_ZC," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHSL_ETFHB," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN" ||
                "                        ABS(B.YSSL)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSGSL_ETFHB," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL * A.ZXJ)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHJE_SS_ETFHB," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN" ||
                "                        ABS(B.YSSL * A.ZXJ)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSGJE_SF_ETFHB," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '30' AND IS_ETFHB = 1 AND YSSL < 0 THEN" ||
                "                        ABS(B.YSSL * A.ZXJ)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSHJE_SJ_ETFHB," ||
                "                 SUM(CASE" ||
                "                       WHEN JYLB = '29' AND IS_ETFHB = 1 AND YSSL > 0 THEN" ||
                "                        ABS(B.YSSL * A.ZXJ)" ||
                "                       ELSE" ||
                "                        0" ||
                "                     END) AS DRSGJE_SJ_ETFHB " ||
                "            FROM (SELECT jys
										  ,zqdm
										  ,zqmc
										  ,jydw
										  ,CASE WHEN JYS IN (8,9) THEN ZXJ*F_GET_HLCS(2,RQ) ELSE zxj END AS ZXJ
										  ,zsp
										  ,jkp
										  ,zgj
										  ,zdj
										  ,cjsl
										  ,cjje
										  ,zxlx
										  ,lxjg
										  ,jjjybz
										  ,zxj_org
										  ,zsp_org
										  ,gzj_flag
										  ,rq FROM dsc_bas.T_ZQHQ_XZ_HIS WHERE RQ = "|| I_RQ || ") A," ||   --20210125
                "                 (SELECT JG.TZZH_ID," ||
                "                         JG.KHH," ||
                "                         JG.GDH," ||
                "                         JG.JYS," ||
                "                         JG.ZQDM," ||
                "                         JG.JYLB," ||
                "                         JG.ZQLB," ||
                "                         JG.CJBH," ||
                "                         JG.BZ," ||
                "                         JG.CJRQ," ||
                "                         JG.CJSL," ||
                "                         JG.CJJE," ||
                "                         JG.YSSL," ||
                "                         JG.YSJE," ||
                "                         JG.S1," ||
                "                         JG.S2," ||
                "                         JG.S3," ||
                "                         JG.S4," ||
                "                         JG.S5," ||
                "                         JG.S6," ||
                "                         CASE WHEN PG.ZQDM IS NOT NULL THEN 1 ELSE 0 END IS_PG," ||
                "                         CASE WHEN PG.PGJG IS NOT NULL THEN PG.PGJG ELSE 0 END PGJG," ||
                "                         CASE WHEN KG.ZQDM IS NOT NULL THEN 1 ELSE 0 END IS_KG," ||
                "                         CASE WHEN D.JJDM IS NOT NULL AND JG.JYS != '2' THEN 1 ELSE 0 END IS_ETFHB," ||
               "                          CASE WHEN XSB.GPDM IS NOT NULL THEN 1 ELSE 0 END IS_XSB_JXC, "||  -- 20200728 新增新三板精选层判断
				"						  XSB.FXJ AS XSB_FXJ "||  -- 获取申购代码发行价，调整持仓成本
               -- "                         CASE WHEN E.KHH IS NOT NULL THEN 1 ELSE 0 END IS_ETFRG," ||
                --"                         CASE WHEN E.KHH IS NOT NULL THEN E.CCCB ELSE 0 END ETFRG_CB " ||
                "                    FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQS', I_KHH) || " JG " ||
                "                    LEFT JOIN CUST.T_HBJJXX D ON (JG.JYS=D.JYS AND JG.ZQDM=D.JJDM)" ||
                "                    LEFT JOIN (select distinct cqcxrq,jys,zqdm from INFO.TGP_GSFHKG WHERE cqcxrq=" || I_RQ || " and jys='2') kg ON (kg.jys=jg.jys and kg.zqdm=jg.zqdm)" ||
                "                    LEFT JOIN (select distinct pgssrq,jys,zqdm,pgjg from INFO.TGP_GPPG WHERE pgssrq=" || l_Day1 || ") pg ON (pg.jys=jg.jys and pg.zqdm=jg.zqdm)" ||
				"                   LEFT JOIN INFO.TXGGL XSB ON XSB.SSBK = 'XSB' AND JG.ZQDM = XSB.GPDM AND XSB.SSRQ="|| l_Day1 ||
                --"                    LEFT JOIN (select KHH, GDH, JYS, ZQDM, SUM(YSJE_2) AS CCCB, SUM(CJSL) AS YSSL FROM DSC_BAS.T_DJSQSZL_HIS WHERE RQ=" || l_lastDay || " AND JYS = '2' AND JYLB IN ('32', '41') AND ZQLB = 'E3' GROUP BY KHH,GDH,JYS,ZQDM) E ON (E.KHH = JG.KHH AND E.GDH = JG.GDH AND E.JYS = JG.JYS AND CAST((CAST(E.ZQDM AS INT)-4) AS STRING)=JG.ZQDM AND E.YSSL = JG.YSSL)" ||             
                "                   WHERE JG.JYLB IN ('7'," ||
                "                                     '9'," ||
                "                                     '10'," ||
                "                                     '12'," ||
                "                                     '13'," ||
                "                                     '15'," ||
                "                                     '18'," ||
                "                                     '19'," ||
                "                                     '31'," ||
                "                                     '37'," ||
                "                                     '38'," ||
                "                                     '47'," ||
                "                                     '48'," ||
                "                                     '65'," ||
                "                                     '66'," ||
                "                                     '21'," ||
                "                                     '20')" ||
                "                     AND JG.ZQDM NOT IN ('888880', '900000') " ||
                "                     AND JG.CJBH NOT IN " ||
                -- CJBH为“新股上市”，是深圳首发上市，JYLB为83上面已处理
                -- CJBH为“上市转入”，是深圳配股、送股、增发上市使用，其中配股在下面处理，送股不处理成本
                -- CJBH为“新股上市、配股上市、送股上市”、"送股托管"、"新股托管"为上海使用，后面要处理成本
                -- 排除掉T+0交易货基的申购赎回处理机制
                "                         ('折算变更', '比例变更') " ||
                "                     AND JG.CJBH NOT IN (" || 
                "                                         '配股上市'," ||
                "                                         '送股上市'," ||
                "                                         '送股托管'," ||
                "                                         '新股托管')) B" ||
                "           WHERE A.RQ = B.CJRQ" ||
                "             AND A.JYS = B.JYS" ||
                "             AND A.ZQDM = B.ZQDM" ||
                "           GROUP BY B.TZZH_ID, B.KHH, B.GDH, B.JYS, B.ZQDM, B.BZ) A";
                  
   -- 数据插入至资产统计临时表sparkZqyeCbjs_0
    l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH) || l_columns || l_sqlBuf;
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
                "   FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) || 
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='19' AND CJBH='份额变动' " ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";
        l_tableName := F_IDS_GET_TABLENAME('sparkJjshFebd', I_KHH);
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        
        --43 赎回确认
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL" ||
                 "   FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) || 
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='43' AND CJBH='赎回确认'" ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";
         
        l_tableName := F_IDS_GET_TABLENAME('sparkJjshqr', I_KHH);
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        
        --待交收
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL," ||
                "        SUM(YSJE_2) AS YSJE" ||
                "   FROM DSC_BAS.T_DJSQSZL_HIS " ||
                "   WHERE ZQLB='L0' AND JYS='2' AND JYLB='43' AND RQ = CJRQ AND RQ = " || I_RQ ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";
         
        l_tableName := F_IDS_GET_TABLENAME('sparkJjshDjs', I_KHH);
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        
        --汇总
        l_sqlBuf := " SELECT  0 as TZZH_ID," ||
                    I_RQ || " AS RQ," ||
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
                "         0 AS DRSHJE_SS_ETFHB," ||
                "         0 AS DRSGJE_SF_ETFHB" ||
                "      FROM " || F_IDS_GET_TABLENAME('sparkJjshFebd', I_KHH) || " A " ||
                " LEFT JOIN "|| F_IDS_GET_TABLENAME('sparkJjshDjs', I_KHH) || " D ON (A.KHH=D.KHH AND A.GDH=D.GDH AND A.BZ=D.BZ AND A.JYS=D.JYS AND A.ZQDM=D.ZQDM AND A.CJSL=D.CJSL)" ||
                "     WHERE NOT EXISTS (SELECT 1 FROM "|| F_IDS_GET_TABLENAME('sparkJjshqr', I_KHH) || " B WHERE A.KHH=B.KHH AND A.GDH=B.GDH AND A.BZ=B.BZ AND A.JYS=B.JYS AND A.ZQDM=B.ZQDM AND A.CJSL=B.CJSL)";
        
        --写入当日成本计算表
        l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH) || l_columns || l_sqlBuf;
        EXECUTE IMMEDIATE l_sql;
    END;
    
    BEGIN
        --深圳LOF赎回顺延
        l_sqlBuf := "SELECT KHH," ||
                "        GDH," ||
                "        BZ," ||
                "        JYS," ||
                "        ZQDM," ||
                "        SUM(CJSL) AS CJSL," ||
                "        SUM(YSJE_2) AS YSJE" ||
                "   FROM dsc_bas.t_djsqszl_his " ||
                "   WHERE ZQLB='L0' AND JYS='1' AND JYLB='43' AND YSJE>0 AND CJSL>0 AND RQ=CJRQ AND RQ= " || I_RQ ||
                "   GROUP BY KHH,GDH,JYS,ZQDM,BZ";
        l_tableName := F_IDS_GET_TABLENAME('sparkJjszDjs', I_KHH);
        
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        
        l_sqlBuf :=  "SELECT  0 as TZZH_ID," || I_RQ ||
                "         AS RQ," ||
                "         KHH," ||
                "         JYS," ||
                "         GDH," ||
                "         ZQDM," ||
                "         BZ," ||
                "         0-CJSL AS DRYSSL," ||
                "         0 AS DRMRJE," ||
                "         0 AS DRMRSL," ||
                "         YSJE AS DRMCJE," ||
                "         CJSL AS DRMCSL," ||
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
                " from " || F_IDS_GET_TABLENAME('sparkJjszDjs', I_KHH);
                
         --写入当日成本计算表
        l_sql := 'INSERT INTO TABLE ' || F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH) || l_columns || l_sqlBuf;
        EXECUTE IMMEDIATE l_sql;
    END;

-----------------------------    //8、生成成本计算数据-----------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkCbjsResult', I_KHH);
    l_sqlBuf := "SELECT " ||
                "  A.TZZH_ID," ||
                "  A.KHH," ||
                "  A.GDH," ||
                "  A.JYS," ||
                "  A.ZQDM," ||
                "  A.ZQLB," ||
                "  A.BZ," ||
                "  CAST(NVL(NVL((CASE WHEN (A.DRMRSL>0 AND A.DRMRSL=A.LJMRSL AND A.ZQSL>0) OR (A.DRMRSL>0 AND A.DRMRSL=A.LJMRSL AND A.DRMRSL=A.DRMCSL AND A.ZQSL = 0) THEN A.RQ" ||
                "       ELSE NULL" ||
                "  END),A.KCRQ)," || I_RQ || " ) AS INT) AS KCRQ," ||
                "  A.ZQSL," ||
                "  A.ZQSZ," ||
                "  A.DRYSSL," ||
                "  A.DRMRJE," ||
                "  A.DRMRSL," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS DRPGJE," ||
                "  A.DRPGSL," ||
                "  A.DRMCJE," ||
                "  A.DRMCSL," ||
                "  A.DRJYFY," ||
                "  CAST(NULL AS DECIMAL(16,3)) AS SRCCJJ," ||
                "  A.LJMRJE," ||
                "  A.LJMRSL," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS LJPGJE," ||
                "  A.LJPGSL," ||
                "  A.LJMCJE," ||
                "  A.LJMCSL," ||
                "  A.LJJYFY," ||
                "  A.SRZQSL," ||
                "  A.SRZQSZ," ||
                "  ROUND(A.SRCBJ, 4) AS SRCBJ," ||
                "  A.SRCCCB," ||
                "  A.SRLJYK," ||
                "  CAST(ROUND(CASE WHEN A.DRMRSL> A.DRMCSL THEN A.SRCCCB+A.DRMRJE-A.DRMCJE" ||
                "             WHEN A.DRMRSL< A.DRMCSL THEN A.SRCCCB-A.SRCBJ*(A.DRMCSL-A.DRMRSL)" ||
                "             ELSE A.SRCCCB" ||
                "         END,2) AS DECIMAL(16,2)) AS CCCB," ||
                "  CAST(ROUND(CASE WHEN A.DRMRSL> A.DRMCSL THEN A.SRLJYK" ||
                "             WHEN A.DRMRSL< A.DRMCSL THEN A.SRLJYK+(A.DRMCJE-A.DRMRJE)-A.SRCBJ*(A.DRMCSL-A.DRMRSL)" ||
                "             ELSE A.SRLJYK+(A.DRMCJE-A.DRMRJE)" ||
                "         END,2) AS DECIMAL(16,2)) AS LJYK," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS CCCB_TZ," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS LJYK_TZ," ||
                "  A.DRHBCFJE_ZR," ||
                "  A.DRHBCFSL_ZR," ||
                "  A.DRHBCFJE_ZC," ||
                "  A.DRHBCFSL_ZC," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS DRHBCFJE_ZR_TZ," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS DRHBCFJE_ZC_TZ," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZR," ||
                "  CAST(NULL AS DECIMAL(16,2)) AS YK_ZC," ||
                "  A.DRSHDQSL," ||
                "  A.DRSHDQJE," ||
                "  A.DRSHSYSL," ||
                "  A.SRSHSYSL," ||
                "  CASE WHEN A.ZQSL=0 AND (A.DRMCSL>0 OR (A.DRSHDQSL>0 AND A.SRZQSL>0 AND A.DRSHDQSL<>A.SRSHSYSL)) AND A.DRSHSL_ETFHB>0 THEN " || I_RQ ||
                "       WHEN A.ZQSL=0 AND (A.DRMCSL>0 OR (A.DRSHDQSL>0 AND A.SRZQSL>0 AND A.DRSHDQSL<>A.SRSHSYSL)) THEN " || I_RQ ||
                "       ELSE 0" ||
                "  END AS QCRQ, " ||
                "  A.DRSHSL_ETFHB," ||
                "  A.DRSGSL_ETFHB," ||
                "  A.DRSHJE_SJ_ETFHB," ||
                "  A.DRSGJE_SJ_ETFHB," ||
                "  A.DRSHJE_SS_ETFHB," ||
                "  A.DRSGJE_SF_ETFHB," ||
                "  A.LJSHJE_SJ_ETFHB," ||
                "  A.LJSGJE_SJ_ETFHB," ||
                "  A.LJSHJE_SS_ETFHB," ||
                "  A.LJSGJE_SF_ETFHB," ||
                I_RQ || " AS RQ" ||
                "    FROM " ||
                " (SELECT A.TZZH_ID," || I_RQ ||
                "           AS RQ," ||
                "           A.KHH," ||
                "           A.JYS," ||
                "           A.GDH," ||
                "           A.ZQDM," ||
                "           Z.ZQLB AS ZQLB," ||
                "           A.BZ," ||
                "           A.KCRQ," ||
                "           NVL(B.ZQSL,0) AS ZQSL," ||
                "           NVL(B.ZXSZ,0) AS ZQSZ," ||
                "           CAST(NVL(DRYSSL,0) AS DECIMAL(22,2)) AS DRYSSL," ||
                "           CAST(NVL(DRMRJE,0) AS DECIMAL(16,2)) AS DRMRJE," ||
                "           CAST(NVL(DRMRSL,0) AS DECIMAL(22,2)) AS DRMRSL," ||
                "           CAST(NVL(DRMCJE,0) AS DECIMAL(16,2)) AS DRMCJE," ||
                "           CAST(NVL(DRMCSL,0) AS DECIMAL(22,2)) AS DRMCSL," ||
                "           CAST(NVL(DRPGSL,0) AS DECIMAL(22,2)) AS DRPGSL," ||
                "           CAST(NVL(DRJYFY,0) AS DECIMAL(16,2)) AS DRJYFY," ||
                "           CAST(NVL(DRHBCFJE_ZR,0) AS DECIMAL(16,2)) AS DRHBCFJE_ZR," ||
                "           CAST(NVL(DRHBCFSL_ZR,0) AS DECIMAL(16,2)) AS DRHBCFSL_ZR," ||
                "           CAST(NVL(DRHBCFJE_ZC,0) AS DECIMAL(16,2)) AS DRHBCFJE_ZC," ||
                "           CAST(NVL(DRHBCFSL_ZC,0) AS DECIMAL(16,2)) AS DRHBCFSL_ZC," ||
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMRJE,0),NVL(LJMRJE,0)) AS DECIMAL(16,2)) AS LJMRJE," || //-- 考虑当日买入卖出清仓情况处理
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMRSL,0),NVL(LJMRSL,0)) AS DECIMAL(22,2)) AS LJMRSL," || //-- 考虑当日买入卖出清仓情况处理
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMCJE,0),NVL(LJMCJE,0)) AS DECIMAL(16,2)) AS LJMCJE," || //-- 考虑当日买入卖出清仓情况处理
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRMCSL,0),NVL(LJMCSL,0)) AS DECIMAL(22,2)) AS LJMCSL," || //-- 考虑当日买入卖出清仓情况处理
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRPGSL,0),NVL(LJPGSL,0)) AS DECIMAL(22,2)) AS LJPGSL," || //-- 考虑当日买入卖出清仓情况处理
                "           CAST(IF(NVL(C.ZQSL,0)=0 AND NVL(DRMRSL,0)=NVL(DRMCSL,0),NVL(DRJYFY,0),NVL(LJJYFY,0)) AS DECIMAL(16,2)) AS LJJYFY," || //-- 考虑当日买入卖出清仓情况处理
                "           NVL(C.CCCB,0) SRCCCB," ||
                "           NVL(C.LJYK,0) SRLJYK," ||
                "           NVL(C.ZQSL,0) SRZQSL," ||
                "           NVL(C.ZXSZ,0) SRZQSZ," ||
                "           IF(NVL(C.ZQSL,0) > 0, NVL(C.CCCB,0)/NVL(C.ZQSL,0), 0) AS SRCBJ," ||
                "           CAST(NVL(DRSHDQSL,0) AS DECIMAL(16,2)) AS DRSHDQSL," ||
                "           CAST(NVL(DRSHDQJE,0) AS DECIMAL(16,2)) AS DRSHDQJE," ||
                "           CAST(NVL(DRSHSYSL,0) AS DECIMAL(16,2)) AS DRSHSYSL," ||
                "           CAST(NVL(SRSHSYSL,0) AS DECIMAL(16,2)) AS SRSHSYSL," ||
                "           CAST(NVL(DRSHSL_ETFHB,0) AS DECIMAL(16,2)) AS DRSHSL_ETFHB," ||
                "           CAST(NVL(DRSGSL_ETFHB,0) AS DECIMAL(16,2)) AS DRSGSL_ETFHB," ||
                "           CAST(NVL(DRSHJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS DRSHJE_SJ_ETFHB," ||
                "           CAST(NVL(DRSGJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS DRSGJE_SJ_ETFHB," ||
                "           CAST(NVL(LJSHJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS LJSHJE_SJ_ETFHB," ||
                "           CAST(NVL(LJSGJE_SJ_ETFHB,0) AS DECIMAL(16,2)) AS LJSGJE_SJ_ETFHB," ||
                "           CAST(NVL(DRSHJE_SS_ETFHB,0) AS DECIMAL(16,2)) AS DRSHJE_SS_ETFHB," ||
                "           CAST(NVL(DRSGJE_SF_ETFHB,0) AS DECIMAL(16,2)) AS DRSGJE_SF_ETFHB," ||
                "           CAST(NVL(LJSHJE_SS_ETFHB,0) AS DECIMAL(16,2)) AS LJSHJE_SS_ETFHB," ||
                "           CAST(NVL(LJSGJE_SF_ETFHB,0) AS DECIMAL(16,2)) AS LJSGJE_SF_ETFHB" ||
                "     FROM (SELECT D.TZZH_ID," ||
                "           D.KHH," ||
                "           D.JYS," ||
                "           D.GDH," ||
                "           D.ZQDM," ||
                "           D.BZ," ||
                "           NVL(SUM(KCRQ),0) KCRQ," ||
                "           NVL(SUM(DRYSSL),0) DRYSSL," ||
                "           NVL(SUM(DRMRJE),0) DRMRJE," ||
                "           NVL(SUM(DRMRSL),0) DRMRSL," ||
                "           NVL(SUM(DRMCJE),0) DRMCJE," ||
                "           NVL(SUM(DRMCSL),0) DRMCSL," ||
                "           NVL(SUM(DRPGSL),0) DRPGSL," ||
                "           NVL(SUM(DRJYFY),0) DRJYFY," ||
                "           NVL(SUM(DRHBCFJE_ZR),0) DRHBCFJE_ZR," ||
                "           NVL(SUM(DRHBCFSL_ZR),0) DRHBCFSL_ZR," ||
                "           NVL(SUM(DRHBCFJE_ZC),0) DRHBCFJE_ZC," ||
                "           NVL(SUM(DRHBCFSL_ZC),0) DRHBCFSL_ZC," ||
                "           NVL(SUM(LJMRJE),0) LJMRJE," ||
                "           NVL(SUM(LJMRSL),0) LJMRSL," ||
                "           NVL(SUM(LJMCJE),0) LJMCJE," ||
                "           NVL(SUM(LJMCSL),0) LJMCSL," ||
                "           NVL(SUM(LJPGSL),0) LJPGSL," ||
                "           NVL(SUM(LJJYFY),0) LJJYFY," ||
                "           NVL(SUM(DRSHDQSL),0) DRSHDQSL," ||
                "           NVL(SUM(DRSHDQJE),0) DRSHDQJE," ||
                "           NVL(SUM(DRSHSYSL),0) DRSHSYSL," ||
                "           NVL(SUM(SRSHSYSL),0) SRSHSYSL," ||
                "           NVL(SUM(DRSHSL_ETFHB),0) AS DRSHSL_ETFHB," ||
                "           NVL(SUM(DRSGSL_ETFHB),0) AS DRSGSL_ETFHB," ||
                "           NVL(SUM(DRSHJE_SJ_ETFHB),0) AS DRSHJE_SJ_ETFHB," ||
                "           NVL(SUM(DRSGJE_SJ_ETFHB),0) AS DRSGJE_SJ_ETFHB," ||
                "           NVL(SUM(LJSHJE_SJ_ETFHB),0) AS LJSHJE_SJ_ETFHB," ||
                "           NVL(SUM(LJSGJE_SJ_ETFHB),0) AS LJSGJE_SJ_ETFHB," ||
                "           NVL(SUM(DRSHJE_SS_ETFHB),0) AS DRSHJE_SS_ETFHB," ||
                "           NVL(SUM(DRSGJE_SF_ETFHB),0) AS DRSGJE_SF_ETFHB," ||
                "           NVL(SUM(LJSHJE_SS_ETFHB),0) AS LJSHJE_SS_ETFHB," ||
                "           NVL(SUM(LJSGJE_SF_ETFHB),0) AS LJSGJE_SF_ETFHB" ||
                "      FROM (SELECT TZZH_ID," ||
                "                   KHH," ||
                "                   JYS," ||
                "                   GDH," ||
                "                   ZQDM," ||
                "                   BZ," ||
                "                   0 as KCRQ," ||
                "                   SUM(DRYSSL) AS DRYSSL," ||
                "                   SUM(DRMRJE) AS DRMRJE," ||
                "                   SUM(DRMRSL) AS DRMRSL," ||
                "                   SUM(DRMCJE) AS DRMCJE," ||
                "                   SUM(DRMCSL) AS DRMCSL," ||
                "                   SUM(DRPGSL) AS DRPGSL," ||
                "                   SUM(DRJYFY) AS DRJYFY," ||
                "                   SUM(DRHBCFJE_ZR) AS DRHBCFJE_ZR," ||
                "                   SUM(DRHBCFSL_ZR) AS DRHBCFSL_ZR," ||
                "                   SUM(DRHBCFJE_ZC) AS DRHBCFJE_ZC," ||
                "                   SUM(DRHBCFSL_ZC) AS DRHBCFSL_ZC," ||
                "                   SUM(DRMRJE) AS LJMRJE," ||
                "                   SUM(DRMRSL) AS LJMRSL," ||
                "                   SUM(DRMCJE) AS LJMCJE," ||
                "                   SUM(DRMCSL) AS LJMCSL," ||
                "                   SUM(DRPGSL) AS LJPGSL," ||
                "                   SUM(DRJYFY) AS LJJYFY," ||
                "                   SUM(DRSHDQSL) AS DRSHDQSL," ||
                "                   SUM(DRSHDQJE) AS DRSHDQJE," ||
                "                   SUM(DRSHSYSL) AS DRSHSYSL," ||
                "                   SUM(SRSHSYSL) AS SRSHSYSL," ||
                "                   SUM(DRSHSL_ETFHB) AS DRSHSL_ETFHB," ||
                "                   SUM(DRSGSL_ETFHB) AS DRSGSL_ETFHB," ||
                "                   SUM(DRSHJE_SJ_ETFHB) AS DRSHJE_SJ_ETFHB," ||
                "                   SUM(DRSGJE_SJ_ETFHB) AS DRSGJE_SJ_ETFHB," ||
                "                   SUM(DRSHJE_SJ_ETFHB) AS LJSHJE_SJ_ETFHB," ||
                "                   SUM(DRSGJE_SJ_ETFHB) AS LJSGJE_SJ_ETFHB," ||
                "                   SUM(DRSHJE_SS_ETFHB) AS DRSHJE_SS_ETFHB," ||
                "                   SUM(DRSGJE_SF_ETFHB) AS DRSGJE_SF_ETFHB," ||
                "                   SUM(DRSHJE_SS_ETFHB) AS LJSHJE_SS_ETFHB," ||
                "                   SUM(DRSGJE_SF_ETFHB) AS LJSGJE_SF_ETFHB" ||
                "              FROM " || F_IDS_GET_TABLENAME('sparkZqyeCbjs_0', I_KHH)  ||
                "             GROUP BY TZZH_ID, KHH, JYS, GDH, ZQDM,BZ" ||
                "            UNION ALL" ||
                "            SELECT TZZH_ID," ||
                "                   KHH," ||
                "                   JYS," ||
                "                   GDH," ||
                "                   ZQDM," ||
                "                   BZ," ||
                "                   KCRQ," ||
                "                   0       AS DRYSSL," ||
                "                   0       AS DRMRJE," ||
                "                   0       AS DRMRSL," ||
                "                   0       AS DRMCJE," ||
                "                   0       AS DRMCSL," ||
                "                   0       AS DRPGSL," ||
                "                   0       AS DRJYFY," ||
                "                   0 AS DRHBCFJE_ZR," ||
                "                   0 AS DRHBCFSL_ZR," ||
                "                   0 AS DRHBCFJE_ZC," ||
                "                   0 AS DRHBCFSL_ZC," ||
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJMRJE,0)) LJMRJE," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJMRSL,0)) LJMRSL," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJMCJE,0)) LJMCJE," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJMCSL,0)) LJMCSL," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJPGSL,0)) LJPGSL," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJJYFY,0)) LJJYFY," || //-- 上一日清仓的累计值就不能累计到当日
                "                   0 AS DRSHDQSL," ||
                "                   0 AS DRSHDQJE," ||
                "                   0 AS DRSHSYSL," ||
                "                   0 AS SRSHSYSL," ||
                "                   0 AS DRSHSL_ETFHB," ||
                "                   0 AS DRSGSL_ETFHB," ||
                "                   0 AS DRSHJE_SJ_ETFHB," ||
                "                   0 AS DRSGJE_SJ_ETFHB," ||
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJSHJE_SJ_ETFHB,0)) LJSHJE_SJ_ETFHB," || //-- 上一日清仓的累计值就不能累计到当日
                "                   IF(NVL(ZQSL,0)=0,0,NVL(LJSGJE_SJ_ETFHB,0)) LJSGJE_SJ_ETFHB," || //-- 上一日清仓的累计值就不能累计到当日
                "                   0 AS DRSHJE_SS_ETFHB," ||
                "                   0 AS DRSGJE_SF_ETFHB," ||
                "                   LJSHJE_SS_ETFHB," ||
                "                   LJSGJE_SF_ETFHB " ||
                "              FROM " || F_IDS_GET_TABLENAME('sparkCbjsPre1', I_KHH) || " where zqsl!=0" ||
                "             ) D            " ||
                "     GROUP BY D.TZZH_ID, D.BZ, D.KHH, D.JYS, D.GDH, D.ZQDM) A LEFT JOIN" ||
                "                  " || F_IDS_GET_TABLENAME('sparkZqyeDR', I_KHH) || " B " ||
                "      on (A.JYS=B.JYS AND A.ZQDM=B.ZQDM AND A.GDH=B.GDH AND A.TZZH_ID=B.TZZH_ID AND A.KHH = B.KHH)" ||
                "                  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) || " c" ||
                "      on (A.JYS=c.JYS AND A.ZQDM=c.ZQDM AND A.GDH=c.GDH AND A.TZZH_ID=c.TZZH_ID AND A.KHH = C.KHH)" ||
                "                  LEFT JOIN cust.t_zqdm Z " ||
                "      ON (A.JYS=Z.JYS AND A.ZQDM=Z.ZQDM))A";   
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
  
-----------------------      //9、更新到表-----------------------------------------------
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname,targetTable, partitionValue, khh)
     */ 
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_ZQYE_CBJS", I_RQ, I_KHH);
  END; 


------------------------------------ //10、更新证券余额表中的CCCB和LJYK----------------------------------------
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeResult', I_KHH);
    l_sqlBuf := "SELECT Z.TZZH_ID," ||
                "         Z.KHH," ||
                "         Z.JYS," ||
                "         Z.GDH," ||
                "         Z.ZQDM," ||
                "         Z.KCRQ," ||
                "         Z.ZQLB," ||
                "         Z.BZ," ||
                "         Z.ZQSL," ||
                "         Z.FLTSL," ||
                "         Z.ZXSZ," ||
                "         Z.CCCB," ||
                "         CAST((CASE" ||
                "                WHEN Z.ZQSL <> 0 THEN" ||
                "                 ROUND(IF(Z.ZQSL = 0, 0, Z.CCCB / Z.ZQSL), 4)" ||
                "                ELSE" ||
                "                 Z.CBJ" ||
                "              END) AS DECIMAL(9, 4)) AS CBJ," ||
                "         Z.LJYK," ||
                "         CAST((CASE" ||
                "                WHEN Z.ZQSL <> 0 THEN" ||
                "                 Z.CCCB - Z.LJYK" ||
                "                ELSE" ||
                "                 Z.TBCCCB" ||
                "              END) AS DECIMAL(16, 2)) AS TBCCCB," ||
                "         CAST((CASE" ||
                "                WHEN Z.ZQSL <> 0 THEN" ||
                "                 ROUND(IF(Z.ZQSL = 0, 0, (Z.CCCB - Z.LJYK) / Z.ZQSL), 4)" ||
                "                ELSE" ||
                "                 Z.TBCBJ" ||
                "              END) AS DECIMAL(16, 4)) AS TBCBJ," ||
                "         Z.DRYK," ||
                "         Z.ZQSL_ORIG," ||
                I_RQ || " AS RQ" ||
                "    FROM (SELECT Z.TZZH_ID," ||
                "                 Z.RQ," ||
                "                 Z.KHH," ||
                "                 Z.JYS," ||
                "                 Z.GDH," ||
                "                 Z.ZQDM," ||
                "                 CASE" ||
                "                   WHEN C.KHH IS NOT NULL THEN" ||
                "                    C.KCRQ" ||
                "                   ELSE" ||
                "                    Z.KCRQ" ||
                "                 END AS KCRQ," ||
                "                 Z.ZQLB," ||
                "                 Z.BZ," ||
                "                 Z.ZQSL," ||
                "                 Z.FLTSL," ||
                "                 Z.ZXSZ," ||
                "                 CASE" ||
                "                   WHEN C.KHH IS NOT NULL THEN" ||
                "                    NVL(C.CCCB, 0)" ||
                "                   ELSE" ||
                "                    Z.CCCB" ||
                "                 END AS CCCB," ||
                "                 Z.CBJ," ||
                "                 CASE" ||
                "                   WHEN C.KHH IS NOT NULL THEN" ||
                "                    NVL(C.LJYK, 0)" ||
                "                   ELSE" ||
                "                    Z.LJYK" ||
                "                 END AS LJYK," ||
                "                 Z.TBCCCB," ||
                "                 Z.TBCBJ," ||
                "                 CAST(CASE" ||
                "                        WHEN C.KHH IS NOT NULL THEN" ||
                "                         NVL(C.ZQSZ - C.SRZQSZ - (C.DRMRJE - C.DRMCJE), 0)" ||
                "                        ELSE" ||
                "                         Z.DRYK" ||
                "                      END AS DECIMAL(16, 2)) AS DRYK," ||
                "                 Z.ZQSL_ORIG" ||
                "            FROM " || F_IDS_GET_TABLENAME('sparkZqyeDR', I_KHH) || " Z" ||
                "            LEFT JOIN " || F_IDS_GET_TABLENAME('sparkCbjsResult', I_KHH) || " C" ||
                "              ON (Z.JYS = C.JYS AND Z.ZQDM = C.ZQDM AND" ||
                "                 Z.GDH = C.GDH AND Z.TZZH_ID = C.TZZH_ID AND Z.KHH = C.KHH) ) Z" ||
                "    LEFT JOIN CUST.T_ZQDM D" ||
                "      ON (Z.JYS = D.JYS AND Z.ZQDM = D.ZQDM)";    
                  
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

  END;
  
  BEGIN
    /**
     * 写入分区表
     * 入参：临时表名，目标库名，目标表名，目标表分区字段值，客户号
     * F_IDS_OVERWRITE_PARTITION(tablename, dbname, targetTable, partitionValue, khh)
     */ 
    F_IDS_OVERWRITE_PARTITION(l_tableName, "CUST", "T_ZQYE_HIS", I_RQ, I_KHH);
  END;
  
  BEGIN 
    --调用成本计算扩展
    P_IDS_COST_CALCUATION_EXT(I_RQ, I_KHH);
  END;
end;
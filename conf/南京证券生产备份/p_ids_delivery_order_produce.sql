CREATE OR REPLACE PROCEDURE cust.p_ids_delivery_order_produce(
--输入变量
I_RQ IN INT,
I_KHH IN STRING
)
IS
/******************************************************************
  *文件名称：CUST.P_IDS_DELIVERY_ORDER_PRODUCE
  *项目名称：IDS计算
  *文件说明：集中交易-交割单清算后处理

  创建人：燕居庆
  功能说明：集中交易-交割单清算后处理

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        2019/6/18       创建
  燕居庆        v1.0.1        2019/9/13       对标java-ids的4858版本；
                                              1.调整通过1.统一处理交割单的CJJE计算,增加37/38类别
*******************************************************************/
l_sqlBuf STRING;    --创建表语句
l_tableName STRING; --临时表名
l_sqlWhereCurrentDay STRING; 
l_sqlWhereLastDay STRING;
l_lastDay INT;
BEGIN
  -- 获取上一交易日
  SELECT F_GET_JYR_DATE(I_RQ, -1) INTO l_lastDay FROM system.dual;
  
  IF I_KHH IS NULL THEN
    l_sqlWhereCurrentDay := I_RQ;
    l_sqlWhereLastDay := l_lastDay || ' ';
  ELSE 
    l_sqlWhereCurrentDay := I_RQ || ' and khh = \'' || I_KHH || '\'';
    l_sqlWhereLastDay := l_lastDay || ' and khh = \'' || I_KHH || '\'';
  END IF;
  
  --获取源数据
  --获取当日交割单
  BEGIN 
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_JGMXLS_HIS WHERE CJRQ = ' || l_sqlWhereCurrentDay;
    
    /**
     * 调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
     */
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --获取委托流水
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkWtlsHis', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_WTLS_HIS WHERE WTRQ = ' || l_sqlWhereCurrentDay;
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --获取上日持仓
  --获取上日证券余额
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH);
    l_sqlBuf := 'SELECT * FROM CUST.T_ZQYE_HIS WHERE RQ=' || l_sqlWhereLastDay;
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --统一处理交割单，不区分分级基金
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsAll', I_KHH);
    l_sqlBuf := "SELECT  T.LSH,T.TZZH_ID,T.SEQNO,T.WTH,T.KHH,T.KHXM,T.GDH,T.JYS,T.BZ,T.YYB,T.ZQDM,T.ZQMC,T.ZQLB,T.JYLB," ||
                " CASE WHEN T.JYLB = '43' AND T.cjrq>T.JSRQ AND T.JSRQ<>0 THEN 'SHJSCG' " ||
                "      WHEN T.JYLB = '43' AND T.JSRQ=0 THEN 'SHFQWT' " ||
                "      WHEN T.JYLB = '30' AND T.cjrq<T.JSRQ THEN 'SHFQWT' " ||
                "      WHEN T.JYLB = '42' AND T.JSRQ=0 THEN 'SGFQWT' " ||
                "      WHEN T.JYLB = '11' AND T.YSSL>0 AND ZZ.GPDM IS NOT NULL THEN CONCAT(ZZ.ZQDM,T.CJBH) " ||
                " ELSE T.CJBH END AS CJBH," ||
                " T.cjrq,T.CJSJ,T.SBSJ,T.CJBS,T.CJSL,T.CJJG,T.JSJ,T.LXJG," ||
                " (CASE WHEN ((T.ZQLB IN ('A0','C0','A3','C3') AND T.JYLB IN ('18','19'))) AND T.CJJE=0 THEN T.CJSL*Z.ZXJ " ||
                "       WHEN T.JYLB IN ('65','66','7','10','31','9','21','20','15','12', '37', '38') AND T.CJJE=0 THEN T.CJSL*Z.ZXJ" ||
                " ELSE T.CJJE END) AS CJJE,T.LXJE," ||
                " (CASE WHEN T.CJJE=0 AND T.ZQLB='E0' AND T.JYLB='30' AND H.JJDM IS NOT NULL THEN T.CJSL*NVL(H.JJJZ,100)" ||
                " WHEN T.ZQLB IN ('A0','C0','A3','C3') AND T.JYLB IN ('18','19') AND T.CJJE=0 THEN T.CJSL*Z.ZXJ " ||
                " ELSE T.YSJE END) AS YSJE, T.JSRQ,T.BCZQYE," ||
                " (CASE WHEN T.JYS='2' AND T.ZQLB='L0' AND T.JYLB='42' AND T.JSRQ!=0 AND T.YSSL=0 THEN T.CJSL ELSE T.YSSL END) AS YSSL," ||
                " CASE WHEN T.ZQLB IN ('EH') AND T.JYLB='29' THEN 20000000000+T.SEQNO  WHEN T.JYLB = '94' THEN 10000000000+T.SEQNO ELSE IF(T.WTH=0,10000000000+T.SEQNO,T.WTH) END  AS SEQNO_QS," ||
                " T.BCZQYE AS BCZQYE_QS, T.CJSJ AS CJSJ_QS,0 as CCCB_QS,0 as CBJ_QS,0 as SXYK_QS,0 as LJYK_QS," ||
                " T.JYLB AS JYLB_QS,0 AS ZT_QS, 0 AS ROWNUM,S1,S2,S3,S4,S5,S6,ROUND(Z.ZXJ, 4) AS ZXJ," ||
                " (CASE WHEN J.JJDM IS NOT NULL THEN J.JJJZ WHEN T.CJJE=0 AND T.ZQLB='E0' AND T.JYLB='30' AND H.JJDM IS NOT NULL THEN NVL(H.JJJZ,100) ELSE NULL END) AS JJJZ" ||
                "  FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) ||"  T
                  LEFT JOIN (SELECT * FROM DSC_BAS.T_ZQHQ_XZ_HIS WHERE RQ = "|| I_RQ||") Z
                    ON (Z.JYS = T.JYS AND Z.ZQDM = T.ZQDM)
                  LEFT JOIN (SELECT * FROM INFO.THIS_JJJZ_HBJJ WHERE jzrq = "|| I_RQ ||" )  H
                    ON (H.TADM = T.JYS AND H.JJDM = T.ZQDM)
                  LEFT JOIN (SELECT * FROM DSC_BAS.T_FJJJJZ_HIS WHERE JZRQ = "|| I_RQ || ") J
                    ON (J.TADM = T.JYS AND J.JJDM = T.ZQDM)
                  LEFT JOIN (SELECT
                    DISTINCT ZQDM
                              , GPDM,sgrq,
                              lead(sgrq,1,99999999)
                              over(PARTITION BY gpdm ORDER BY sgrq) id
                FROM
                    INFO.TZQ_KZZ) ZZ ON (T.ZQDM = ZZ.GPDM  AND t.cjrq >= zz.sgrq AND t.cjrq < zz.id)
                 WHERE NOT (T.ZQLB IN ('E0') AND T.JYLB = '29' AND T.SEQNO = 0)
                   AND NOT (T.ZQLB IN ('EH') AND T.JYLB = '30' AND T.CJSL = 0) ";
     
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  -- 由委托流水生成分级基金数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkWtlsFjjj', I_KHH);
    l_sqlBuf := "SELECT A.*,
                       CASE
                           WHEN COUNT(1) OVER(PARTITION BY A.KHH, JYS, ZQDM, JYLB, CJSL) = 1 THEN
                            0  /*分析函数COUNT(1) 需要有1参数*/
                           ELSE
                            ROW_NUMBER() OVER(PARTITION BY A.KHH, JYS, ZQDM, JYLB, CJSL ORDER BY WTH DESC)
                       END AS ROWNUM
                  FROM (SELECT T.TZZH_ID,
                               T.SEQNO,
                               T.WTH,
                               T.KHH,
                               T.KHXM,
                               T.GDH,
                               T.JYS,
                               T.BZ,
                               D.JJDM AS ZQDM,
                               (CASE
                                   WHEN D.JJLX = '1' THEN
                                    T.ZQMC
                                   ELSE
                                    D.JJMC
                               END) AS ZQMC,
                               T.ZQLB,
                               T.JYLB,
                               (CASE
                                   WHEN T.JYLB = '47' THEN
                                    '基金拆分'
                                   WHEN T.JYLB = '48' THEN
                                    '基金合并'
                               END) AS CJBH,
                               T.WTRQ AS CJRQ,
                               (CASE
                                   WHEN T.JYS = '1' THEN
                                    T.WTSJ
                                   ELSE
                                    T.CJSJ
                               END) AS CJSJ,
                               (CASE
                                   WHEN T.JYS = '1' AND D.JJLX = '1' THEN
                                    T.WTSL
                                   WHEN T.JYS = '2' AND D.JJLX = '1' THEN
                                    T.CJSL
                                   WHEN T.JYS = '1' AND D.JJLX <> '1' THEN
                                    T.WTSL * CAST(D.ABBL AS DECIMAL) / 10
                                   WHEN T.JYS = '2' AND D.JJLX <> '1' THEN
                                    T.CJSL * CAST(D.ABBL AS DECIMAL) / 10
                                   ELSE
                                    T.CJSL
                               END) AS CJSL,
                               0 AS BCZQYE,
                               CASE
                                   WHEN D.JJLX = '1' THEN
                                    T.CJSL
                                   ELSE
                                    T.CJSL * CAST(D.ABBL AS DECIMAL) / 10
                               END AS YSSL
                          FROM "|| F_IDS_GET_TABLENAME('sparkWtlsHis', I_KHH) || " T,  cust.v_fjjjxx D
                         WHERE T.JYS = D.JYS
                           AND T.ZQDM = D.MJDM
                           AND T.WTRQ = " || I_RQ || "
                           AND T.JYLB IN ('47', '48')
                           AND T.SBJG <> '8') A
                 ORDER BY A.KHH, JYS, ZQDM, JYLB, CJSL, WTH DESC";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  -- 单独生成分级基金的交割单供后续处理
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjj', I_KHH);
    l_sqlBuf := "SELECT T.LSH,
                       T.TZZH_ID,
                       T.SEQNO,
                       T.WTH,
                       T.KHH,
                       T.KHXM,
                       T.GDH,
                       T.JYS,
                       T.BZ,
                       T.YYB,
                       T.ZQDM,
                       T.ZQMC,
                       T.ZQLB,
                       T.JYLB,
                       CASE
                           WHEN T.JYLB = '43' AND T.CJRQ > T.JSRQ AND T.JSRQ <> 0 THEN
                            'SHJSCG'
                           WHEN T.JYLB = '43' AND T.JSRQ = 0 THEN
                            'SHFQWT'
                           WHEN T.JYLB = '30' AND T.CJRQ < T.JSRQ THEN
                            'SHFQWT'
                           WHEN T.JYLB = '42' AND T.JSRQ = 0 THEN
                            'SGFQWT'
                           ELSE
                            T.CJBH
                       END AS CJBH,
                       T.CJRQ,
                       T.CJSJ,
                       T.SBSJ,
                       T.CJBS,
                       T.CJSL,
                       T.CJJG,
                       T.JSJ,
                       T.LXJG,
                       T.CJJE,
                       T.LXJE,
                       T.YSJE,
                       T.JSRQ,
                       T.BCZQYE,
                       T.YSSL AS YSSL,
                       CASE
                           WHEN T.ZQLB IN ('EH') AND T.JYLB = '29' THEN
                            20000000000 + T.SEQNO
                           WHEN (T.JYLB = '19' AND T.CJBH = '折算变更' AND T.ZQLB = 'L3' AND D.JJLX = '1') OR
                                (T.JYLB = '18' AND T.CJBH = '折算变更' AND T.ZQLB = 'A0' AND D.JJLX <> '1') THEN
                            " || I_RQ || "
                           ELSE
                            T.WTH
                       END AS SEQNO_QS,
                       T.BCZQYE AS BCZQYE_QS,
                       T.CJSJ AS CJSJ_QS,
                       0 AS CCCB_QS,
                       0 AS CBJ_QS,
                       0 AS SXYK_QS,
                       0 AS LJYK_QS,
                       CASE
                           WHEN (T.JYLB = '18' AND T.CJBH = '拆分合并' AND D.JJLX = '1') OR
                                (T.JYLB = '19' AND T.CJBH = '拆分合并' AND D.JJLX <> '1') THEN
                            '48'
                           WHEN (T.JYLB = '19' AND T.CJBH = '拆分合并' AND D.JJLX = '1') OR
                                (T.JYLB = '18' AND T.CJBH = '拆分合并' AND D.JJLX <> '1') THEN
                            '47'
                           WHEN (T.JYLB = '19' AND T.CJBH = '折算变更' AND T.ZQLB = 'L3' AND D.JJLX = '1') THEN
                            '47'
                           WHEN (T.JYLB = '18' AND T.CJBH = '折算变更' AND T.ZQLB = 'A0' AND D.JJLX <> '1') THEN
                            '47'
                           ELSE
                            T.JYLB
                       END AS JYLB_QS,
                       0 AS ZT_QS,
                       0 AS ROWNUM,
                       S1,
                       S2,
                       S3,
                       S4,
                       S5,
                       S6,
                       T.ZXJ,
                       T.JJJZ
                  FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAll', I_KHH) || " T,  cust.v_fjjjxx D
                 WHERE T.JYS = D.JYS
                   AND T.ZQDM = D.JJDM";
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  -- 分级基金合并拆分排序
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjjSort', I_KHH);
    l_sqlBuf := "SELECT F.LSH,
                       F.TZZH_ID,
                       F.SEQNO,
                       F.WTH,
                       F.KHH,
                       F.KHXM,
                       F.GDH,
                       F.JYS,
                       F.BZ,
                       F.YYB,
                       F.ZQDM,
                       F.ZQMC,
                       F.ZQLB,
                       F.JYLB,
                       F.CJBH,
                       F.CJRQ,
                       F.CJSJ,
                       F.SBSJ,
                       F.CJBS,
                       F.CJSL,
                       F.CJJG,
                       F.JSJ,
                       F.LXJG,
                       F.CJJE,
                       F.LXJE,
                       F.YSJE,
                       F.JSRQ,
                       F.BCZQYE,
                       F.YSSL,
                       (CASE
                           WHEN F.JYLB_QS IN ('47', '48') AND F.SEQNO_QS = 0 AND W.KHH IS NOT NULL THEN
                            IF(W.WTH = 0, 10000000000 + F.SEQNO, W.WTH)
                           ELSE
                            IF(F.SEQNO_QS = 0, 10000000000 + F.SEQNO, F.SEQNO_QS)
                       END) AS SEQNO_QS,
                       F.BCZQYE_QS,
                       (CASE
                           WHEN F.JYLB_QS IN ('47', '48') AND F.SEQNO_QS = 0 AND W.KHH IS NOT NULL THEN
                            W.CJSJ
                           ELSE
                            F.CJSJ_QS
                       END) AS CJSJ_QS,
                       F.CCCB_QS,
                       F.CBJ_QS,
                       F.SXYK_QS,
                       F.LJYK_QS,
                       F.JYLB_QS,
                       F.ZT_QS,
                       F.ROWNUM,
                       F.S1,
                       F.S2,
                       F.S3,
                       F.S4,
                       F.S5,
                       F.S6,
                       F.ZXJ,
                       F.JJJZ
                  FROM (SELECT F.LSH,
                               F.TZZH_ID,
                               F.SEQNO,
                               F.WTH,
                               F.KHH,
                               F.KHXM,
                               F.GDH,
                               F.JYS,
                               F.BZ,
                               F.YYB,
                               F.ZQDM,
                               F.ZQMC,
                               F.ZQLB,
                               F.JYLB,
                               F.CJBH,
                               F.CJRQ,
                               F.CJSJ,
                               F.SBSJ,
                               F.CJBS,
                               F.CJSL,
                               F.CJJG,
                               F.JSJ,
                               F.LXJG,
                               F.CJJE,
                               F.LXJE,
                               F.YSJE,
                               F.JSRQ,
                               F.BCZQYE,
                               F.YSSL,
                               F.SEQNO_QS,
                               F.BCZQYE_QS,
                               F.CJSJ_QS,
                               F.CCCB_QS,
                               F.CBJ_QS,
                               F.SXYK_QS,
                               F.LJYK_QS,
                               F.JYLB_QS,
                               F.ZT_QS,
                               (CASE
                                   WHEN T.KHH IS NULL THEN
                                    F.ROWNUM
                                   ELSE
                                    ROW_NUMBER() OVER(PARTITION BY F.KHH,
                                                      F.CJRQ,
                                                      F.JYS,
                                                      F.ZQDM,
                                                      F.JYLB,
                                                      F.CJSL ORDER BY F.WTH DESC)
                               END) AS ROWNUM,
                               F.S1,
                               F.S2,
                               F.S3,
                               F.S4,
                               F.S5,
                               F.S6,
                               F.ZXJ,
                               F.JJJZ
                          FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjj', I_KHH) ||" F
                          LEFT JOIN (SELECT T.KHH, T.JYS, T.ZQDM, T.JYLB, T.CJSL, COUNT(*) AS CNT
                                      FROM "|| F_IDS_GET_TABLENAME('sparkWtlsFjjj', I_KHH) ||" T
                                     WHERE T.JYLB IN ('47', '48')
                                     GROUP BY T.KHH, T.CJRQ, T.JYS, T.ZQDM, T.JYLB, CJSL
                                    HAVING COUNT(*) > 1) T
                            ON (F.KHH = T.KHH AND F.JYS = T.JYS AND F.ZQDM = T.ZQDM AND F.JYLB = T.JYLB AND
                               F.CJSL = T.CJSL)) F
                  LEFT JOIN "||F_IDS_GET_TABLENAME('sparkWtlsFjjj', I_KHH) || " W
                    ON (W.KHH = F.KHH AND W.TZZH_ID = F.TZZH_ID AND W.GDH = F.GDH AND W.JYS = F.JYS AND
                       W.ZQDM = F.ZQDM AND W.JYLB = F.JYLB AND W.ROWNUM = F.ROWNUM AND W.CJSL = F.CJSL)";
                       
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  -- 对于流水中同一只代码SEQNO
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsAllSort', I_KHH);
    l_sqlBuf := "SELECT T.LSH,
                       T.TZZH_ID,
                       T.SEQNO,
                       T.WTH,
                       T.KHH,
                       T.KHXM,
                       T.GDH,
                       T.JYS,
                       T.BZ,
                       T.YYB,
                       T.ZQDM,
                       T.ZQMC,
                       T.ZQLB,
                       T.JYLB,
                       T.CJBH,
                       T.CJRQ,
                       T.CJSJ,
                       T.SBSJ,
                       T.CJBS,
                       T.CJSL,
                       T.CJJG,
                       T.JSJ,
                       T.LXJG,
                       T.CJJE,
                       T.LXJE,
                       T.YSJE,
                       T.JSRQ,
                       T.BCZQYE,
                       T.YSSL,
                       (CASE
                           WHEN A.KHH IS NOT NULL AND T.SEQNO_QS = 10000000000 THEN
                            T.SEQNO_QS + A.RN
                           ELSE
                            T.SEQNO_QS
                       END) AS SEQNO_QS,
                       T.BCZQYE_QS,
                       T.CJSJ_QS,
                       T.CCCB_QS,
                       T.CBJ_QS,
                       T.SXYK_QS,
                       T.LJYK_QS,
                       T.JYLB_QS,
                       T.ZT_QS,
                       T.ROWNUM,
                       T.S1,
                       T.S2,
                       T.S3,
                       T.S4,
                       T.S5,
                       T.S6,
                       T.ZXJ,
                       T.JJJZ
                  FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAll', I_KHH) || " T 
                  LEFT JOIN (SELECT A.*, ROW_NUMBER() OVER(PARTITION BY A.KHH ORDER BY A.KHH) RN
                               FROM (SELECT A.KHH, A.JYS, A.ZQDM, A.SEQNO_QS, COUNT(1) AS CNT
                                       FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAll', I_KHH) || " A
                                       /* LEFT ANTI
                                       JOIN cust.v_fjjjxx F
                                         ON (A.JYS = F.JYS AND A.ZQDM = F.JJDM) */
                                      WHERE A.SEQNO_QS = 10000000000 AND NOT EXISTS (SELECT 1 FROM cust.v_fjjjxx F WHERE a.jys = f.jys AND A.ZQDM = F.JJDM )
                                      GROUP BY A.KHH, A.JYS, A.ZQDM, A.SEQNO_QS
                                     HAVING COUNT(1) > 1) A) A
                    ON (T.KHH = A.KHH AND T.JYS = A.JYS AND T.ZQDM = A.ZQDM)";
                    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --数据合并查询
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkjgmxlsQsAllHB', I_KHH);
    l_sqlBuf := "SELECT LSH,
                       TZZH_ID,
                       SEQNO,
                       WTH,
                       KHH,
                       KHXM,
                       GDH,
                       JYS,
                       BZ,
                       YYB,
                       ZQDM,
                       ZQMC,
                       ZQLB,
                       JYLB,
                       CJBH,
                       CJRQ,
                       CJSJ,
                       SBSJ,
                       CJBS,
                       CJSL,
                       CJJG,
                       JSJ,
                       LXJG,
                       CJJE,
                       LXJE,
                       YSJE,
                       JSRQ,
                       BCZQYE,
                       YSSL,
                       SEQNO_QS,
                       BCZQYE_QS,
                       CJSJ_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_QS,
                       CAST(0 AS DECIMAL(9, 4)) AS CBJ_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS SXYK_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS LJYK_QS,
                       JYLB_QS,
                       ZT_QS,
                       ROWNUM,
                       S1,
                       S2,
                       S3,
                       S4,
                       S5,
                       S6,
                       ZXJ,
                       JJJZ,
                       CAST(NULL AS STRING) AS REMARK_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_ZC,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_ZR,
                       CAST(0 AS DECIMAL(16, 2)) AS YK_ZC,
                       CAST(0 AS DECIMAL(16, 2)) AS YK_ZR,
                       CAST(0 AS DECIMAL(16, 2)) AS JYFY
                  FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAllSort', I_KHH) ||"  A
                  /*LEFT ANTI
                  JOIN cust.v_fjjjxx F
                    ON (A.JYS = F.JYS AND A.ZQDM = F.JJDM)*/
                  WHERE  NOT EXISTS (SELECT 1 FROM cust.v_fjjjxx F WHERE a.jys = f.jys AND A.ZQDM = F.JJDM )
                UNION ALL
                SELECT LSH,
                       TZZH_ID,
                       SEQNO,
                       WTH,
                       KHH,
                       KHXM,
                       GDH,
                       JYS,
                       BZ,
                       YYB,
                       ZQDM,
                       ZQMC,
                       ZQLB,
                       JYLB,
                       CJBH,
                       CJRQ,
                       CJSJ,
                       SBSJ,
                       CJBS,
                       CJSL,
                       CJJG,
                       JSJ,
                       LXJG,
                       CJJE,
                       LXJE,
                       YSJE,
                       JSRQ,
                       BCZQYE,
                       YSSL,
                       SEQNO_QS,
                       BCZQYE_QS,
                       CJSJ_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_QS,
                       CAST(0 AS DECIMAL(9, 4)) AS CBJ_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS SXYK_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS LJYK_QS,
                       JYLB_QS,
                       ZT_QS,
                       ROWNUM,
                       S1,
                       S2,
                       S3,
                       S4,
                       S5,
                       S6,
                       ZXJ,
                       JJJZ,
                       CAST(NULL AS STRING) AS REMARK_QS,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_ZC,
                       CAST(0 AS DECIMAL(16, 2)) AS CCCB_ZR,
                       CAST(0 AS DECIMAL(16, 2)) AS YK_ZC,
                       CAST(0 AS DECIMAL(16, 2)) AS YK_ZR,
                       CAST(0 AS DECIMAL(16, 2)) AS JYFY
                  FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjjSort', I_KHH) || "
                 WHERE GDH IS NOT NULL
                   AND JYS IS NOT NULL
                   AND ZQDM IS NOT NULL
                   AND SEQNO_QS IS NOT NULL";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --成本迭代
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkDeliveryHoding', I_KHH);
    l_sqlBuf := "SELECT T.*,
                     A.ZQSL AS ZQSL_CC,
                     A.CCCB AS CCCB_CC,
                     A.LJYK AS LJYK_CC,
                     A.CBJ  AS CBJ_CC,
                     B.KHH  AS FJ_KHH
                FROM " || F_IDS_GET_TABLENAME('sparkjgmxlsQsAllHB', I_KHH) || " T
                LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) || " A
                  ON (T.KHH = A.KHH AND T.GDH = A.GDH AND T.JYS = A.JYS AND T.ZQDM = A.ZQDM)
                LEFT JOIN (SELECT DISTINCT KHH, GDH, JYS, ZQDM
                             FROM " || F_IDS_GET_TABLENAME('sparkjgmxlsQsAllHB', I_KHH) || "
                            WHERE JYLB IN ('47', '48')) B
                  ON (T.KHH = B.KHH AND T.GDH = B.GDH AND T.JYS = B.JYS AND T.ZQDM = B.ZQDM)";
     
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  end;
  
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsCccb', I_KHH);
    l_sqlBuf := "
        SELECT 
            DeliveryHoding(row)
        FROM 
            (SELECT 
                 khh
                  , groupRow(lsh
                              , tzzh_id
                              , seqno
                              , wth
                              , khh
                              , khxm
                              , gdh
                              , jys
                              , bz
                              , yyb
                              , zqdm
                              , zqmc
                              , zqlb
                              , jylb
                              , cjbh
                              , cjrq
                              , cjsj
                              , sbsj
                              , cjbs
                              , cjsl
                              , cjjg
                              , jsj
                              , lxjg
                              , cjje
                              , lxje
                              , ysje
                              , jsrq
                              , bczqye
                              , yssl
                              , seqno_qs
                              , bczqye_qs
                              , cjsj_qs
                              , cccb_qs
                              , cbj_qs
                              , sxyk_qs
                              , ljyk_qs
                              , jylb_qs
                              , zt_qs
                              , rownum
                              , s1
                              , s2
                              , s3
                              , s4
                              , s5
                              , s6
                              , zxj
                              , jjjz
                              , remark_qs
                              , cccb_zc
                              , cccb_zr
                              , yk_zc
                              , yk_zr
                              , jyfy
                              , zqsl_cc
                              , cccb_cc
                              , ljyk_cc
                              , cbj_cc
                              , fj_khh) AS  ROW 
             FROM " || F_IDS_GET_TABLENAME('sparkDeliveryHoding', I_KHH) ||"
             GROUP  BY 
                  khh) A";
    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
 --修正后的持仓成本,顺带更新盲拆的清算序列号
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsAllXz', I_KHH);
    l_sqlBuf := "SELECT TZZH_ID,
                       SEQNO,
                       WTH,
                       T.KHH,
                       KHXM,
                       GDH,
                       T.JYS,
                       BZ,
                       YYB,
                       T.ZQDM,
                       ZQMC,
                       ZQLB,
                       JYLB,
                       CJBH,
                       CJSJ,
                       SBSJ,
                       CJBS,
                       CJSL,
                       CJJG,
                       JSJ,
                       LXJG,
                       CJJE,
                       LXJE,
                       YSJE,
                       JSRQ,
                       BCZQYE,
                       YSSL,
                       ZXJ,
                       JJJZ,
                       (CASE
                           WHEN D.KHH IS NOT NULL AND YSSL != 0 AND JYLB = '42' THEN
                            1
                           ELSE
                            SEQNO_QS
                       END) AS SEQNO_QS,
                       BCZQYE_QS,
                       CJSJ_QS,
                       CCCB_QS,
                       CBJ_QS,
                       SXYK_QS,
                       LJYK_QS,
                       REMARK_QS,
                       JYLB_QS,
                       ZT_QS,
                       YK_ZC,
                       YK_ZR,
                       LSH,
                       ROWNUM,
                       S1,
                       S2,
                       S3,
                       S4,
                       S5,
                       S6,
                       JYFY,
                       CCCB_ZC,
                       CCCB_ZR,
                       CJRQ
                  FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsCccb', I_KHH) ||" T
                  LEFT JOIN (SELECT KHH, JYS, ZQDM
                               FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) || "
                              WHERE JYLB = '47'
                                 OR (JYLB = '19' AND CJBH = '拆分合并')
                              GROUP BY KHH, JYS, ZQDM) D
                    ON (T.KHH = D.KHH AND T.JYS = D.JYS AND T.ZQDM = D.ZQDM)";
                    
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --重新生成分级基金合并拆分的交割数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjj', I_KHH);
    l_sqlBuf := "SELECT J.*
                FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAllXz', I_KHH) ||" J, cust.v_fjjjxx F
               WHERE J.JYS = F.JYS
                 AND J.ZQDM = F.JJDM
                 AND J.ZT_QS = 0";
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --根据清算交割单进行分级基金三个代码合一处理
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsQsone', I_KHH);
    l_sqlBuf := "SELECT T.TZZH_ID,
                       T.KHH,
                       T.GDH,
                       T.SEQNO_QS  AS SEQNO_QS,
                       T.JYLB_QS,
                       T.CJRQ,
                       T.CJBH,
                       T.JYS,
                       T.ZQDM_FJMJ,
                       CAST(T.FLAG AS INT) AS FLAG,
                       T.MJDM,
                       T.MJDM_CJJG,
                       T.MJDM_YSSL,
                       T.MJDM_YSJE,
                       (CASE
                           WHEN M.ZQDM IS NOT NULL THEN
                            NVL(M.ZQSL, 0)
                           ELSE
                            T.MJDM_BCYE
                       END) AS MJDM_BCYE,
                       (CASE
                           WHEN M.ZQDM IS NOT NULL THEN
                            NVL(M.CCCB, 0)
                           ELSE
                            T.MJDM_CCCB
                       END) AS MJDM_CCCB,
                       (CASE
                           WHEN M.ZQDM IS NOT NULL THEN
                            NVL(M.CBJ, 0)
                           ELSE
                            T.MJDM_CBJ
                       END) AS MJDM_CBJ,
                       T.MJDM_DCYK,
                       (CASE
                           WHEN M.ZQDM IS NOT NULL THEN
                            NVL(M.LJYK, 0)
                           ELSE
                            T.MJDM_LJYK
                       END) AS MJDM_LJYK,
                       T.AJDM,
                       T.AJDM_CJJG,
                       T.AJDM_YSSL,
                       T.AJDM_YSJE,
                       (CASE
                           WHEN A.ZQDM IS NOT NULL THEN
                            NVL(A.ZQSL, 0)
                           ELSE
                            T.AJDM_BCYE
                       END) AS AJDM_BCYE,
                       (CASE
                           WHEN A.ZQDM IS NOT NULL THEN
                            NVL(A.CCCB, 0)
                           ELSE
                            T.AJDM_CCCB
                       END) AS AJDM_CCCB,
                       (CASE
                           WHEN A.ZQDM IS NOT NULL THEN
                            NVL(A.CBJ, 0)
                           ELSE
                            T.AJDM_CBJ
                       END) AS AJDM_CBJ,
                       T.AJDM_DCYK,
                       (CASE
                           WHEN A.ZQDM IS NOT NULL THEN
                            NVL(A.LJYK, 0)
                           ELSE
                            T.AJDM_LJYK
                       END) AS AJDM_LJYK,
                       T.BJDM,
                       T.BJDM_CJJG,
                       T.BJDM_YSSL,
                       T.BJDM_YSJE,
                       (CASE
                           WHEN B.ZQDM IS NOT NULL THEN
                            NVL(B.ZQSL, 0)
                           ELSE
                            T.BJDM_BCYE
                       END) AS BJDM_BCYE,
                       (CASE
                           WHEN B.ZQDM IS NOT NULL THEN
                            NVL(B.CCCB, 0)
                           ELSE
                            T.BJDM_CCCB
                       END) AS BJDM_CCCB,
                       (CASE
                           WHEN B.ZQDM IS NOT NULL THEN
                            NVL(B.CBJ, 0)
                           ELSE
                            T.BJDM_CBJ
                       END) AS BJDM_CBJ,
                       T.BJDM_DCYK,
                       (CASE
                           WHEN B.ZQDM IS NOT NULL THEN
                            NVL(B.LJYK, 0)
                           ELSE
                            T.BJDM_LJYK
                       END) AS BJDM_LJYK,
                       T.CCCB_ZY,
                       T.LJYK_ZY,
                       T.CCCB_ZY_AHM,
                       T.LJYK_ZY_AHM,
                       T.CCCB_ZY_BHM,
                       T.LJYK_ZY_BHM,
                       T.JYFY,
                       CAST('' AS STRING) AS REMARK,
                       ROW_NUMBER() OVER(PARTITION BY T.KHH, T.JYS, T.ZQDM_FJMJ, T.JYLB_QS ORDER BY T.SEQNO_QS) AS RN
                  FROM (SELECT T.TZZH_ID,
                               T.KHH,
                               T.GDH,
                               cast(NVL(T.SEQNO_QS, 0) as bigint) AS SEQNO_QS,
                               T.JYLB_QS,
                               T.CJRQ,
                               MAX(CASE
                                       WHEN T.JYLB_QS = '43' AND CJRQ > JSRQ AND JSRQ <> 0 THEN
                                        'SHJSCG'
                                       WHEN T.JYLB_QS = '43' AND JSRQ = 0 THEN
                                        'SHFQWT'
                                       WHEN T.JYLB_QS = '42' AND JSRQ = 0 THEN
                                        'SGFQWT'
                                       ELSE
                                        JYLB_QS
                                   END) AS CJBH,
                               D.JYS,
                               D.MJDM AS ZQDM_FJMJ,
                               SUM(DISTINCT CASE
                                       WHEN D.JJLX = '1' THEN
                                        1
                                       WHEN D.JJLX = 'A' THEN
                                        2
                                       WHEN D.JJLX = 'B' THEN
                                        4
                                       ELSE
                                        7
                                   END) AS FLAG,
                               MAX(F.MJDM) AS MJDM,
                               MAX(CASE
                                       WHEN D.JJLX = '1' THEN
                                        T.CJJG
                                   END) AS MJDM_CJJG,
                               SUM(CASE
                                       WHEN D.JJLX = '1' THEN
                                        T.YSSL
                                       ELSE
                                        0
                                   END) AS MJDM_YSSL,
                               SUM(CASE
                                       WHEN D.JJLX = '1' THEN
                                        CAST(NVL(T.YSJE, 0.0) AS DECIMAL(16, 2))
                                       ELSE
                                        CAST(0.0 AS DECIMAL(16, 2))
                                   END) AS MJDM_YSJE,
                               MAX(CASE
                                       WHEN D.JJLX = '1' THEN
                                        T.BCZQYE_QS
                                   END) AS MJDM_BCYE,
                               0.00 AS MJDM_CCCB,
                               0.00 AS MJDM_CBJ,
                               0.00 AS MJDM_DCYK,
                               0.00 AS MJDM_LJYK,
                               MAX(F.AJDM) AS AJDM,
                               MAX(CASE
                                       WHEN D.JJLX = 'A' THEN
                                        T.CJJG
                                   END) AS AJDM_CJJG,
                               SUM(CASE
                                       WHEN D.JJLX = 'A' THEN
                                        T.YSSL
                                       ELSE
                                        0
                                   END) AS AJDM_YSSL,
                               ROUND(SUM(CASE
                                             WHEN D.JJLX = 'A' AND T.JYLB_QS IN ('47', '48') THEN
                                              CAST(NVL(CAST(T.YSSL AS DECIMAL(16, 2)) * T.ZXJ, 0) AS DECIMAL(16, 2))
                                             WHEN D.JJLX = 'A' AND T.JYLB_QS NOT IN ('47', '48') THEN
                                              CAST(NVL(T.YSJE, 0) AS DECIMAL(16, 2))
                                             ELSE
                                              CAST(0.0 AS DECIMAL(16, 2))
                                         END),
                                     2) AS AJDM_YSJE,
                               MAX(CASE
                                       WHEN D.JJLX = 'A' THEN
                                        T.BCZQYE_QS
                                   END) AS AJDM_BCYE,
                               0.00 AS AJDM_CCCB,
                               0.00 AS AJDM_CBJ,
                               0.00 AS AJDM_DCYK,
                               0.00 AS AJDM_LJYK,
                               MAX(F.BJDM) AS BJDM,
                               MAX(CASE
                                       WHEN D.JJLX = 'B' THEN
                                        T.CJJG
                                   END) AS BJDM_CJJG,
                               SUM(CASE
                                       WHEN D.JJLX = 'B' THEN
                                        T.YSSL
                                       ELSE
                                        0
                                   END) AS BJDM_YSSL,
                               ROUND(SUM(CASE
                                             WHEN D.JJLX = 'B' AND T.JYLB_QS IN ('47', '48') THEN
                                              CAST(NVL(CAST(T.YSSL AS DECIMAL(16, 2)) * T.ZXJ, 0) AS DECIMAL(16, 2))
                                             WHEN D.JJLX = 'B' AND T.JYLB_QS NOT IN ('47', '48') THEN
                                              CAST(NVL(T.YSJE, 0) AS DECIMAL(16, 2))
                                             ELSE
                                              CAST(0.0 AS DECIMAL(16, 2))
                                         END),
                                     2) AS BJDM_YSJE,
                               MAX(CASE
                                       WHEN D.JJLX = 'B' THEN
                                        T.BCZQYE_QS
                                   END) AS BJDM_BCYE,
                               0.00 AS BJDM_CCCB,
                               0.00 AS BJDM_CBJ,
                               0.00 AS BJDM_DCYK,
                               0.00 AS BJDM_LJYK,
                               0.00 AS CCCB_ZY,
                               0.00 AS LJYK_ZY,
                               0.00 AS CCCB_ZY_AHM,
                               0.00 AS LJYK_ZY_AHM,
                               0.00 AS CCCB_ZY_BHM,
                               0.00 AS LJYK_ZY_BHM,
                               SUM(S1 + S2 + S3 + S4 + S5 + S6) AS JYFY
                          FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjj', I_KHH) ||
                          " T, cust.v_fjjjxx  D,INFO.TFJJJXX F
                         WHERE T.JYS = D.JYS
                           AND T.ZQDM = D.JJDM
                           AND D.JYS = F.JYS
                           AND D.MJDM = F.MJDM
                           AND T.ZT_QS = 0
                         GROUP BY T.TZZH_ID, T.KHH, T.GDH, T.SEQNO_QS, T.JYLB_QS, D.JYS, D.MJDM, T.CJRQ
                        UNION ALL
                        SELECT J.TZZH_ID,
                               J.KHH,
                               J.GDH,
                               CAST(0 AS BIGINT) AS SEQNO_QS,
                               J.JYLB_QS,
                               J.CJRQ,
                               NULL AS CJBH,
                               J.JYS,
                               J.ZQDM_FJMJ,
                               CAST(J.FLAG AS INT) AS FLAG,
                               J.MJDM,
                               0.0 AS MJDM_CJJG,
                               0.0 AS MJDM_YSSL,
                               0.0 AS MJDM_YSJE,
                               0 AS MJDM_BCYE,
                               0 AS MJDM_CCCB,
                               0 AS MJDM_CBJ,
                               NULL AS MJDM_DCYK,
                               0 AS MJDM_LJYK,
                               J.AJDM,
                               0.0 AS AJDM_CJJG,
                               0.0 AS AJDM_YSSL,
                               0.0 AS AJDM_YSJE,
                               0 AS AJDM_BCYE,
                               0 AS AJDM_CCCB,
                               0 AS AJDM_CBJ,
                               NULL AS AJDM_DCYK,
                               0 AS AJDM_LJYK,
                               J.BJDM,
                               0.0 AS BJDM_CJJG,
                               0.0 AS BJDM_YSSL,
                               0.0 AS BJDM_YSJE,
                               0 AS BJDM_BCYE,
                               0 AS BJDM_CCCB,
                               0 AS BJDM_CBJ,
                               NULL AS BJDM_DCYK,
                               0 AS BJDM_LJYK,
                               NULL AS CCCB_ZY,
                               NULL AS LJYK_ZY,
                               NULL AS CCCB_ZY_AHM,
                               NULL AS LJYK_ZY_AHM,
                               NULL AS CCCB_ZY_BHM,
                               NULL AS LJYK_ZY_BHM,
                               NULL AS JYFY
                          FROM (SELECT T.TZZH_ID,
                                       T.KHH,
                                       T.GDH,
                                       0 AS XH,
                                       '0' AS JYLB_QS,
                                       0 AS FLAG,
                                       D.JYS,
                                       D.MJDM AS ZQDM_FJMJ,
                                       F.MJDM,
                                       F.AJDM,
                                       F.BJDM,
                                       T.CJRQ
                                  FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsQsFjjj', I_KHH) ||
                                  " T, cust.v_fjjjxx  D,INFO.TFJJJXX  F
                                 WHERE T.JYS = D.JYS
                                   AND T.ZQDM = D.JJDM
                                   AND D.JYS = F.JYS
                                   AND D.MJDM = F.MJDM
                                 GROUP BY T.TZZH_ID,
                                          T.KHH,
                                          T.GDH,
                                          D.JYS,
                                          D.MJDM,
                                          F.MJDM,
                                          F.AJDM,
                                          F.BJDM,
                                          T.CJRQ) J) T
                  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) ||" M
                    ON (T.KHH = M.KHH AND T.JYS = M.JYS AND T.MJDM = M.ZQDM AND T.TZZH_ID = M.TZZH_ID AND
                       T.GDH = M.GDH)
                  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) || " A
                    ON (T.KHH = A.KHH AND T.JYS = A.JYS AND T.AJDM = A.ZQDM AND T.TZZH_ID = A.TZZH_ID AND
                       T.GDH = A.GDH)
                  LEFT JOIN " || F_IDS_GET_TABLENAME('sparkZqyeSR', I_KHH) || " B
                    ON (T.KHH = B.KHH AND T.JYS = B.JYS AND T.BJDM = B.ZQDM AND T.TZZH_ID = B.TZZH_ID AND
                       T.GDH = B.GDH)";

    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;

  --分级基金迭代
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsJjhbcf', I_KHH);
    l_sqlBuf := "select
                ClassFund(row)
            from
                (select
                     khh
                      , GroupRow(tzzh_id
                                  , khh
                                  , gdh
                                  , seqno_qs
                                  , jylb_qs
                                  , cjrq
                                  , cjbh
                                  , jys
                                  , zqdm_fjmj
                                  , flag
                                  , mjdm
                                  , mjdm_cjjg
                                  , mjdm_yssl
                                  , mjdm_ysje
                                  , mjdm_bcye
                                  , mjdm_cccb
                                  , mjdm_cbj
                                  , mjdm_dcyk
                                  , mjdm_ljyk
                                  , ajdm
                                  , ajdm_cjjg
                                  , ajdm_yssl
                                  , ajdm_ysje
                                  , ajdm_bcye
                                  , ajdm_cccb
                                  , ajdm_cbj
                                  , ajdm_dcyk
                                  , ajdm_ljyk
                                  , bjdm
                                  , bjdm_cjjg
                                  , bjdm_yssl
                                  , bjdm_ysje
                                  , bjdm_bcye
                                  , bjdm_cccb
                                  , bjdm_cbj
                                  , bjdm_dcyk
                                  , bjdm_ljyk
                                  , cccb_zy
                                  , ljyk_zy
                                  , cccb_zy_ahm
                                  , ljyk_zy_ahm
                                  , cccb_zy_bhm
                                  , ljyk_zy_bhm
                                  , jyfy
                                  , remark
                                  , rn) as row
                 from
                     " || F_IDS_GET_TABLENAME('sparkJgmxlsQsone', I_KHH) || "
                 group by
                      khh) A";
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
   -- CUST.P_IDS_FUND_HBCF(I_RQ, I_KHH);
  END;
 --将分级基金修正数据回写回清算交割单中,并修正ZT_QS
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsUpdateHbcf', I_KHH);
    l_sqlBuf := "SELECT J.LSH,
                     J.TZZH_ID,
                     J.SEQNO,
                     J.WTH,
                     J.KHH,
                     J.KHXM,
                     J.GDH,
                     J.JYS,
                     J.BZ,
                     J.YYB,
                     J.ZQDM,
                     J.ZQMC,
                     J.ZQLB,
                     J.JYLB,
                     J.CJBH,
                     J.CJRQ,
                     J.CJSJ,
                     J.SBSJ,
                     J.CJBS,
                     J.CJSL,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          (CASE
                              WHEN M.JYLB_QS IN ('48', '47') THEN
                               M.MJDM_CJJG
                              ELSE
                               J.CJJG
                          END)
                         WHEN A.KHH IS NOT NULL THEN
                          (CASE
                              WHEN A.JYLB_QS IN ('48', '47') THEN
                               A.AJDM_CJJG
                              ELSE
                               J.CJJG
                          END)
                         WHEN B.KHH IS NOT NULL THEN
                          (CASE
                              WHEN B.JYLB_QS IN ('48', '47') THEN
                               B.BJDM_CJJG
                              ELSE
                               J.CJJG
                          END)
                         ELSE
                          J.CJJG
                     END) AS CJJG,
                     J.JSJ,
                     J.LXJG,
                     J.CJJE,
                     J.LXJE,
                     J.YSJE,
                     J.JSRQ,
                     J.BCZQYE,
                     J.YSSL,
                     J.ZXJ,
                     J.JJJZ,
                     J.SEQNO_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL AND M.MJDM_BCYE IS NOT NULL THEN
                          M.MJDM_BCYE
                         WHEN A.KHH IS NOT NULL AND A.AJDM_BCYE IS NOT NULL THEN
                          A.AJDM_BCYE
                         WHEN B.KHH IS NOT NULL AND B.BJDM_BCYE IS NOT NULL THEN
                          B.BJDM_BCYE
                         ELSE
                          J.BCZQYE_QS
                     END) AS BCZQYE_QS,
                     (CASE
                         WHEN (J.CJSJ_QS = '00\:00\:00' OR J.CJSJ_QS IS NULL) AND
                              NOT (J.JYLB = '6' AND J.CJBH <> 'YDJXZ') THEN
                          '18\:00\:00'
                         ELSE
                          J.CJSJ_QS
                     END) AS CJSJ_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          M.MJDM_CCCB
                         WHEN A.KHH IS NOT NULL THEN
                          A.AJDM_CCCB
                         WHEN B.KHH IS NOT NULL THEN
                          B.BJDM_CCCB
                         ELSE
                          J.CCCB_QS
                     END) AS CCCB_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          ROUND(M.MJDM_CBJ, 4)
                         WHEN A.KHH IS NOT NULL THEN
                          ROUND(A.AJDM_CBJ, 4)
                         WHEN B.KHH IS NOT NULL THEN
                          ROUND(B.BJDM_CBJ, 4)
                         ELSE
                          J.CBJ_QS
                     END) AS CBJ_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          M.MJDM_DCYK
                         WHEN A.KHH IS NOT NULL THEN
                          A.AJDM_DCYK
                         WHEN B.KHH IS NOT NULL THEN
                          B.BJDM_DCYK
                         ELSE
                          J.SXYK_QS
                     END) AS SXYK_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          M.MJDM_LJYK
                         WHEN A.KHH IS NOT NULL THEN
                          A.AJDM_LJYK
                         WHEN B.KHH IS NOT NULL THEN
                          B.BJDM_LJYK
                         ELSE
                          J.LJYK_QS
                     END) AS LJYK_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          M.REMARK
                         WHEN A.KHH IS NOT NULL THEN
                          A.REMARK
                         WHEN B.KHH IS NOT NULL THEN
                          B.REMARK
                         ELSE
                          J.REMARK_QS
                     END) AS REMARK_QS,
                     J.JYLB_QS,
                     (CASE
                         WHEN M.KHH IS NOT NULL OR A.KHH IS NOT NULL OR B.KHH IS NOT NULL THEN
                          1
                         WHEN J.CJBH = 'SHFQWT' AND J.JYLB = '43' AND
                              NOT (J.JYLB = '6' AND J.CJBH <> 'YDJXZ') THEN
                          0
                         WHEN J.CJBH = 'SGFQWT' AND J.JYLB = '42' AND
                              NOT (J.JYLB = '6' AND J.CJBH <> 'YDJXZ') THEN
                          0
                         WHEN J.YSSL = 0 AND J.YSJE = 0 AND J.JYLB <> '98' AND
                              NOT (J.JYLB = '6' AND J.CJBH <> 'YDJXZ') THEN
                          0
                         ELSE
                          J.ZT_QS
                     END) AS ZT_QS,
                     J.ROWNUM,
                     J.S1,
                     J.S2,
                     J.S3,
                     J.S4,
                     J.S5,
                     J.S6,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          M.JYFY
                         WHEN A.KHH IS NOT NULL THEN
                          A.JYFY
                         WHEN B.KHH IS NOT NULL THEN
                          B.JYFY
                         ELSE
                          J.JYFY
                     END) AS JYFY,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN M.JYLB_QS = '47' THEN
                                      M.LJYK_ZY
                                 END,
                                 0)
                         WHEN A.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN A.JYLB_QS = '48' THEN
                                      A.LJYK_ZY_AHM
                                 END,
                                 0)
                         WHEN B.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN B.JYLB_QS = '48' THEN
                                      B.LJYK_ZY_BHM
                                 END,
                                 0)
                     END) AS YK_ZC,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN M.JYLB_QS = '48' THEN
                                      M.LJYK_ZY
                                 END,
                                 0)
                         WHEN A.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN A.JYLB_QS = '47' THEN
                                      A.LJYK_ZY_AHM
                                 END,
                                 0)
                         WHEN B.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN B.JYLB_QS = '47' THEN
                                      B.LJYK_ZY_BHM
                                 END,
                                 0)
                     END) AS YK_ZR,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN M.JYLB_QS = '47' THEN
                                      M.CCCB_ZY
                                 END,
                                 0)
                         WHEN A.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN A.JYLB_QS = '48' THEN
                                      A.CCCB_ZY_AHM
                                 END,
                                 0)
                         WHEN B.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN B.JYLB_QS = '48' THEN
                                      B.CCCB_ZY_BHM
                                 END,
                                 0)
                     END) AS CCCB_ZC,
                     (CASE
                         WHEN M.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN M.JYLB_QS = '48' THEN
                                      M.CCCB_ZY
                                 END,
                                 0)
                         WHEN A.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN A.JYLB_QS = '47' THEN
                                      A.CCCB_ZY_AHM
                                 END,
                                 0)
                         WHEN B.KHH IS NOT NULL THEN
                          NVL(CASE
                                     WHEN B.JYLB_QS = '47' THEN
                                      B.CCCB_ZY_BHM
                                 END,
                                 0)
                     END) AS CCCB_ZR
                FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsQsAllXz', I_KHH) || " J
                LEFT JOIN "|| F_IDS_GET_TABLENAME('sparkJgmxlsJjhbcf', I_KHH) || " M
                  ON (J.KHH = M.KHH AND J.TZZH_ID = M.TZZH_ID AND J.GDH = M.GDH AND J.JYS = M.JYS AND
                     J.ZQDM = M.MJDM AND J.SEQNO_QS = M.SEQNO_QS AND M.FLAG <> 0 AND J.CJRQ = M.CJRQ)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('sparkJgmxlsJjhbcf', I_KHH) || " A
                  ON (J.KHH = A.KHH AND J.TZZH_ID = A.TZZH_ID AND J.GDH = A.GDH AND J.JYS = A.JYS AND
                     J.ZQDM = A.AJDM AND J.SEQNO_QS = A.SEQNO_QS AND A.FLAG <> 0 AND J.CJRQ = A.CJRQ)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('sparkJgmxlsJjhbcf', I_KHH) || " B
                  ON (J.KHH = B.KHH AND J.TZZH_ID = B.TZZH_ID AND J.GDH = B.GDH AND J.JYS = B.JYS AND
                     J.ZQDM = B.BJDM AND J.SEQNO_QS = B.SEQNO_QS AND B.FLAG <> 0 AND J.CJRQ = B.CJRQ)";
            
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
  END;
  
  --生成最终数据
  BEGIN
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsResults', I_KHH);
    l_sqlBuf := "SELECT U.*
                  FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsUpdateHbcf', I_KHH) ||" U
                  /*LEFT ANTI
                  JOIN */
                  WHERE NOT EXISTS (SELECT 1 FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) ||" H
                    WHERE H.KHH = U.KHH AND H.CJRQ = U.JSRQ AND H.WTH = U.WTH AND H.JYS = U.JYS AND
                       H.ZQDM = U.ZQDM AND H.CJSL = U.CJSL AND H.JYLB = '43' AND H.CJRQ < U.CJRQ)";
   F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
   
   l_sqlBuf := "INSERT INTO TABLE " || F_IDS_GET_TABLENAME('sparkJgmxlsResults', I_KHH) || 
                "(LSH,
                       TZZH_ID,
                       SEQNO,
                       WTH,
                       KHH,
                       KHXM,
                       GDH,
                       JYS,
                       BZ,
                       YYB,
                       ZQDM,
                       ZQMC,
                       ZQLB,
                       JYLB,
                       CJBH,
                       CJRQ,
                       CJSJ,
                       SBSJ,
                       CJBS,
                       CJSL,
                       CJJG,
                       JSJ,
                       LXJG,
                       CJJE,
                       LXJE,
                       YSJE,
                       JSRQ,
                       BCZQYE,
                       YSSL,
                       ZXJ,
                       JJJZ,
                       SEQNO_QS,
                       BCZQYE_QS,
                       CJSJ_QS,
                       CCCB_QS,
                       CBJ_QS,
                       SXYK_QS,
                       LJYK_QS,
                       REMARK_QS,
                       JYLB_QS,
                       ZT_QS,
                       ROWNUM,
                       S1,
                       S2,
                       S3,
                       S4,
                       S5,
                       S6,
                       JYFY,
                       YK_ZC,
                       YK_ZR,
                       CCCB_ZC,
                       CCCB_ZR) 
                SELECT U.LSH,
                       U.TZZH_ID,
                       U.SEQNO,
                       U.WTH,
                       U.KHH,
                       U.KHXM,
                       U.GDH,
                       U.JYS,
                       U.BZ,
                       U.YYB,
                       U.ZQDM,
                       U.ZQMC,
                       U.ZQLB,
                       U.JYLB,
                       U.CJBH,
                       U.CJRQ,
                       U.CJSJ,
                       U.SBSJ,
                       U.CJBS,
                       U.CJSL,
                       U.CJJG,
                       U.JSJ,
                       U.LXJG,
                       U.CJJE,
                       U.LXJE,
                       U.YSJE,
                       U.JSRQ,
                       U.BCZQYE,
                       U.YSSL,
                       U.ZXJ,
                       U.JJJZ,
                       U.SEQNO_QS,
                       U.BCZQYE_QS,
                       U.CJSJ_QS,
                       U.CCCB_QS,
                       U.CBJ_QS,
                       U.SXYK_QS,
                       U.LJYK_QS,
                       U.REMARK_QS,
                       U.JYLB_QS,
                       (CASE
                           WHEN U.JYLB = '43' AND U.CJBH LIKE 'SHJSCG%' THEN
                            0
                           ELSE
                            U.ZT_QS
                       END) AS ZT_QS,
                       U.ROWNUM,
                       U.S1,
                       U.S2,
                       U.S3,
                       U.S4,
                       U.S5,
                       U.S6,
                       U.JYFY,
                       U.YK_ZC,
                       U.YK_ZR,
                       U.CCCB_ZC,
                       U.CCCB_ZR
                  FROM  "|| F_IDS_GET_TABLENAME('sparkJgmxlsUpdateHbcf', I_KHH) ||" U
                  LEFT SEMI JOIN (SELECT * FROM " || F_IDS_GET_TABLENAME('sparkJgmxlsHis', I_KHH) ||" WHERE JYLB = '43' ) H
                    ON (H.KHH = U.KHH AND H.CJRQ = U.JSRQ AND H.WTH = U.WTH AND H.JYS = U.JYS AND
                       H.ZQDM = U.ZQDM AND H.CJSL = U.CJSL AND H.CJRQ < U.CJRQ)";
    
    EXECUTE IMMEDIATE l_sqlBuf;
    
    l_tableName := F_IDS_GET_TABLENAME('sparkJgmxlsResult', I_KHH);
    l_sqlBuf := " SELECT TZZH_ID,
                          SEQNO,
                          WTH,
                          KHH,
                          KHXM,
                          GDH,
                          JYS,
                          BZ,
                          YYB,
                          ZQDM,
                          ZQMC,
                          ZQLB,
                          JYLB,
                          CJBH,
                          CJSJ,
                          SBSJ,
                          CJBS,
                          CJSL,
                          CAST(CJJG AS DECIMAL(9, 4)) AS CJJG,
                          JSJ,
                          LXJG,
                          CAST(CJJE AS DECIMAL(16, 2)) AS CJJE,
                          LXJE,
                          CAST(YSJE AS DECIMAL(16, 2)) AS YSJE,
                          JSRQ,
                          BCZQYE,
                          YSSL,
                          ZXJ,
                          JJJZ,
                          SEQNO_QS AS SEQNO_QS,
                          CAST(BCZQYE_QS AS DECIMAL(16, 2)) AS BCZQYE_QS,
                          CJSJ_QS,
                          CAST(CCCB_QS AS DECIMAL(16, 2)) AS CCCB_QS,
                          CAST(CBJ_QS AS DECIMAL(10, 4)) AS CBJ_QS,
                          CAST(SXYK_QS AS DECIMAL(16, 2)) AS SXYK_QS,
                          CAST(LJYK_QS AS DECIMAL(16, 2)) AS LJYK_QS,
                          REMARK_QS,
                          JYLB_QS,
                          ZT_QS,
                          CAST(YK_ZC AS DECIMAL(16, 2)) AS YK_ZC,
                          CAST(YK_ZR AS DECIMAL(16, 2)) AS YK_ZR,
                          LSH,
                          CAST(ROWNUM AS INT) AS ROWNUM,
                          S1,
                          S2,
                          S3,
                          S4,
                          S5,
                          S6,
                          CAST(JYFY AS DECIMAL(16, 2)) AS JYFY,
                          CAST(CCCB_ZC AS DECIMAL(16, 2)) AS CCCB_ZC,
                          CAST(CCCB_ZR AS DECIMAL(16, 2)) AS CCCB_ZR," ||
                          I_RQ || " AS CJRQ
                     FROM "|| F_IDS_GET_TABLENAME('sparkJgmxlsResults', I_KHH);
                     
    F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

    F_IDS_OVERWRITE_PARTITION(l_tableName, 'cust', 't_jgmxls_his_qs', I_RQ, I_KHH);
  END;
END;
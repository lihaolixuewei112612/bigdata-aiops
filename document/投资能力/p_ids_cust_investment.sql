create or replace procedure cust.p_ids_cust_investment(
  --输入变量
  I_RQ IN INT, --日期
  I_KHH IN STRING

)

IS
/******************************************************************
  *文件名称：CUST.P_IDS_CUST_INVESTMENT
  *项目名称：IDS计算
  *文件说明：投资分析

  创建人：燕居庆
  功能说明：计算近一月的投资分析

  参数说明

  修改者        版本号        修改日期        说明
  燕居庆        v1.0.0        20200318        创建
*******************************************************************/
l_sj STRING;            --时间
l_j1y INT;
l_ksrq INT;             --开始日期
l_jsrq INT;             --结束日期
l_sqlBuf STRING;        --创建表语句
l_tableName STRING;     --临时表名
l_hlcsHKD decimal(12,6);   --港币汇率
l_hlcsUSD decimal(12,6);   --美元汇率
TYPE nest_table IS TABLE OF STRING;
l_tableArr nest_table DEFAULT NULL; --字段集
l_columns STRING; --字段字符串
l_sql STRING; --执行的sql语句
l_initDate INT; --初始化日期，应当取自配置库
l_month STRING; --年份+月份
BEGIN
    -- 初始化计算参数
    SELECT substr(I_RQ, 1, 6) INTO l_sj from  SYSTEM.DUAL;


    --近1月
    SELECT f_get_jyr_date(f_get_date(I_RQ, 5),0) INTO l_j1y FROM system.dual;20210126
    SELECT f_get_jyr_date(I_RQ,0) INTO l_jsrq FROM system.dual;20210226

    SELECT f_get_hlcs('2', I_RQ) INTO l_hlcsHKD from  system.dual;
    SELECT f_get_hlcs('3', I_RQ) INTO l_hlcsUSD from  system.dual;

    SELECT substr(I_RQ,0,6) INTO l_month FROM system.dual;

    l_ksrq := l_j1y;

    l_initDate := 20180102;

    -- 设置环境，严格字符串
    set_env('character.literal.as.string',true);

    /*
     * 加载hive数据
     * 获取日交易单、持仓、清仓以及部分的交割
     *
     */
    BEGIN
        -- getDailyBillSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH);
            l_sqlBuf := "select * from  cust.t_stat_zd_r where rq between " ||
                        l_ksrq || " AND " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getJzjySecurityBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkJzjyZqye', I_KHH);
            l_sqlBuf := "select * from  cust.t_zqye_his where rq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

         -- getXYSecurityBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkXYzqye', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_zqye_his where rq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getFPProductShare
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkFPcpfe', I_KHH);
            l_sqlBuf := "select * from  cust.t_fp_cpfe_his where rq between " ||
                        l_ksrq || " and "  || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getOptionContractBalanceSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkSoHycc', I_KHH);
            l_sqlBuf := "select * from  cust.t_so_zqye_his where rq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getJzjyInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkJzjyTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_tzsy where qcrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

         -- getXYInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkXyTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_tzsy where qcrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getFPInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkFpTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_fp_tzsy where qcrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getOptionInvestProfit
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkSoTzsy', I_KHH);
            l_sqlBuf := "select * from  cust.t_so_tzsy where qcrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getJzjyDeliveryOrderGTSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkJzjyJgls', I_KHH);
            l_sqlBuf := "select * from  cust.t_jgmxls_his_qs where cjrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- getXYDeliveryOrderSql
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkXyJgls', I_KHH);
            l_sqlBuf := "select * from  cust.t_xy_jgmxls_his_qs where cjrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- 获取期权交割流水
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkQQJgls', I_KHH);
            l_sqlBuf := "select * from  dsc_bas.t_so_jgmxls_his where cjrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        -- 获取金融产品交割流水
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkFpJgls', I_KHH);
            l_sqlBuf := "select * from  dsc_bas.t_fp_jgmxls_his where qrrq between " ||
                        l_ksrq || " and " || l_jsrq || IF(I_KHH IS NULL, "" ," AND KHH = " || I_KHH);

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkZqlb', I_KHH);    --证券类别
            l_sqlBuf := "select * from  DSC_CFG.VW_T_ZQLB_IDS"

            --调用创建临时表函数创建临时表，入参：建表的select语句，临时表名称
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkZcpz', I_KHH);
            l_sqlBuf := "select zqdm,jys,zqlb,zcpzflbm,zcpzflmc from  " ||
                    "(select zqdm,jys,zqlb,zcpzflbm,zcpzflmc,row_number() over(partition by jys,zqdm order by whrq desc) rn " ||
                    "   from  info.TZQDM_ZCPZWH) a where rn=1";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkGsgk', I_KHH);
            l_sqlBuf := "select jys,zqdm,sshy from  (select *,row_number() over(partition by jys,zqdm order by id desc) rn from  "  ||
                        "info.tgp_gsgk where length(sshy)>0 and sshy!='null' and sshy!='无') where rn=1 ";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        --获取存在涨停的股票代码
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_ZQHQ_ZT', I_KHH);
            l_sqlBuf := "SELECT JYS, ZQDM, ZXJ, ZSP, RQ FROM DSC_BAS.T_ZQHQ_HIS WHERE ZXJ/ZSP > 1.0990 AND RQ BETWEEN " || l_ksrq ||" AND " || l_jsrq;
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        --对股票进行分类
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_GPFL', I_KHH);
            l_sqlBuf := "SELECT
                JYS,
                ZQDM,
                CAST(CASE WHEN LTAG>=10000000000 THEN
                '大盘股'
                WHEN LTAG>500000000 AND LTAG<10000000000 THEN
                '中盘股'
                ELSE
                '小盘股'
                END AS STRING)  AS DXP,
                CAST(CASE WHEN S.ZXJ < 10 THEN
                '低价股'
                WHEN S.ZXJ < 20 THEN
                '中价股'
                ELSE
                '高价股'
                END  AS STRING) AS GDJ,
                SSHY
                FROM INFO.TGP_GSGK S";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

       --计算沪深300指数近1月增长率
        begin
            l_tableName := F_IDS_GET_TABLENAME('investment_ZSZZL', NULL);
            l_sqlBuf := "SELECT RQ,ROUND((ZXJ-J1Y_ZXJ) / J1Y_ZXJ, 4) AS ZSZZL FROM (
                SELECT RQ,ZXJ,NVL(LAG(ZXJ) OVER(ORDER BY RQ),ZSP) AS J1Y_ZXJ FROM INFO.THIS_ZSHQ
                WHERE ZSDM='399300' AND RQ IN ("|| l_j1y ||", "|| l_jsrq || ")) WHERE RQ = "|| L_JSRQ;

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        end;
        --1、投资分布
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH);
            l_sqlBuf := "select " ||
                        " khh,"   ||
                        " z.rq,"  ||
                        " z.jys," ||
                        " z.zqdm," ||
                        " nvl(z.zqmc, z.zqdm) as zqmc," ||
                        " dryk," ||
                        " case when z.lb = 'lc' then '理财' when  z.lb='fp' then '基金' when z.lb='so' then '期权' else NVL(a.zqpzmc,'股票') END as zqpz," ||
                        " case when z.lb in ('fp','so') then '权益类' else NVL(b.zcpzflmc, '其他') END as zcpz," ||
                        " regexp_replace(c.sshy,'[,; ]','') as sshy," ||
                        " z.lb," ||
                        " z.fdyk," ||
                        " z.zxsz," ||
                        " z.cccb," ||
                        " A.ZQLBMC " ||
                        " from  " ||
                        "(select khh,z.rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'jzjy' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkJzjyZqye', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'jzjy' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkJzjyTzsy', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union all" ||
                        " select khh,z.rq,z.jrjgdm as jys,z.cpdm as zqdm,d.cpjc as zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast('' as string) as zqlb,(case when (d.cplb in ('607') or z.cpdm = 'A72001') then 'lc' else 'fp' end) as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkFPcpfe', I_KHH) ||" z left join cust.t_jrcpdm d on (z.jrjgdm=d.jrjgdm and z.cpdm=d.cpdm and z.app_id=d.app_id)" ||
                        " union all" ||
                        " select khh,z.qcrq as rq,z.jrjgdm as jys,z.cpdm as zqdm,d.cpjc as zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast('' AS STRING) as zqlb,(case when (d.cplb in ('607') or z.cpdm = 'A72001') then 'lc' else 'fp' end) as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkFpTzsy', I_KHH) ||" z left join cust.t_jrcpdm d on (z.jrjgdm=d.jrjgdm and z.cpdm=d.cpdm and z.app_id=d.app_id)" ||
                        " union all" ||
                        " select khh,z.rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'rzrq' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkXYzqye', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.zqdm,d.zqmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,z.zqlb,'rzrq' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkXyTzsy', I_KHH) ||" z left join cust.t_zqdm d on (z.jys=d.jys and z.zqdm=d.zqdm)" ||
                        " union ALL " ||
                        " select khh,z.rq,z.jys,z.hydm as zqdm,nvl(d.hymc,z.hymc) as zqdmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast(''as string) as zqlb,'so' as lb,CAST(CASE WHEN z.bz = 1 THEN zxsz-tbcccb WHEN z.bz=2 THEN (zxsz-tbcccb) * " || l_hlcsHKD || " WHEN z.bz = 3 THEN (zxsz-tbcccb) * " || l_hlcsUSD || " END AS double) as fdyk,CAST(CASE WHEN z.bz = 1 THEN zxsz WHEN z.bz=2 THEN zxsz * " || l_hlcsHKD || " WHEN z.bz = 3 THEN zxsz * " || l_hlcsUSD || " END AS double) as zxsz,CAST(CASE WHEN z.bz = 1 THEN cccb WHEN z.bz=2 THEN cccb * " || l_hlcsHKD || " WHEN z.bz = 3 THEN cccb * " || l_hlcsUSD || " END AS double) as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkSoHycc', I_KHH) ||" z left join cust.t_so_hydm d on (z.jys=d.jys and z.hydm=d.hydm)" ||
                        " union ALL " ||
                        " select khh,z.qcrq as rq,z.jys,z.hydm as zqdm,nvl(d.hymc,z.hydm) as zqdmc,CAST(CASE WHEN z.bz = 1 THEN dryk WHEN z.bz=2 THEN dryk * " || l_hlcsHKD || " WHEN z.bz = 3 THEN dryk * " || l_hlcsUSD || " END AS double) as dryk,cast(''as string) as zqlb,'so' as lb,0.0 as fdyk,0.0 as zxsz,0.0 as cccb from  " ||
                        F_IDS_GET_TABLENAME('investment_sparkSoTzsy', I_KHH) || " z left join cust.t_so_hydm d on (z.jys=d.jys and z.hydm=d.hydm)) z " ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('investment_sparkZqlb', I_KHH) ||" a on (z.zqlb=a.zqlb and z.jys=a.jys)" ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('investment_sparkZcpz', I_KHH) ||" b on (z.zqdm=b.zqdm and z.jys=b.jys)" ||
                        " left JOIN " ||
                        F_IDS_GET_TABLENAME('investment_sparkGsgk', I_KHH) ||" c on (z.jys=c.jys and z.zqdm=c.zqdm)";


            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
    END;


        --最大回撤
        BEGIN
        l_tableName := F_IDS_GET_TABLENAME('investment_sparkZcfx', I_KHH);
        l_sqlBuf := " SELECT
                 f_ids_month_analyze(row, " ||  l_ksrq || ", " || l_jsrq || ", " ||  l_initDate || " )
            FROM
                (SELECT khh,
                    GroupRow(
                        khh,
                        rq,
                        zzc,
                        zzc_jzjy,
                        zzc_rzrq,
                        zzc_ggqq,
                        zjye,
                        zjye_jzjy,
                        zjye_rzrq,
                        zjye_ggqq,
                        zcjlr,
                        crje,
                        qcje,
                        zrzqsz,
                        zczqsz,
                        yk,
                        yk_jzjy,
                        yk_rzrq,
                        yk_jrcp,
                        yk_ggqq,
                        zqsz,
                        zqsz_jzjy,
                        zqsz_rzrq,
                        zqsz_jrcp,
                        zqsz_ggqq,
                        zfz,
                        zfz_rzrq,
                        zxjz,
                        zxjz_jzjy,
                        zxjz_rzrq,
                        zxjz_ggqq) AS ROW
                        FROM
                        (SELECT khh,
                        rq,
                        zzc,
                        zzc_jzjy,
                        zzc_rzrq,
                        zzc_ggqq,
                        zjye,
                        zjye_jzjy,
                        zjye_rzrq,
                        zjye_ggqq,
                        zcjlr,
                        crje,
                        qcje,
                        zrzqsz,
                        zczqsz,
                        yk,
                        yk_jzjy,
                        yk_rzrq,
                        yk_jrcp,
                        yk_ggqq,
                        zqsz,
                        zqsz_jzjy,
                        zqsz_rzrq,
                        zqsz_jrcp,
                        zqsz_ggqq,
                        zfz,
                        zfz_rzrq,
                        zxjz,
                        zxjz_jzjy,
                        zxjz_rzrq,
                        zxjz_ggqq FROM " ||
                        F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH) || ") GROUP BY khh) a";

        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
    END;

        --计算夏普比率sharp
        --1.计算收益率平均值
        --2.计算标准差
        --3.计算结果
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_sharp', I_KHH);
            l_sqlBuf := "SELECT
            KHH,
            CAST(
                CASE WHEN SYL_STD = 0 THEN
                    0
                ELSE
                    (SYL_AVG - (0.04/30)) / SYL_STD
                END
            AS DECIMAL(12,4)) AS SHARP
            FROM (
                SELECT KHH, AVG(YKL) AS SYL_AVG, STDDEV_POP(YKL) AS SYL_STD
                FROM " || F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH) || " GROUP BY KHH)";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        --平均仓位,最大盈亏，最大亏损
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_CW', I_KHH);
            l_sqlBuf := "
                SELECT KHH, MAX(cast(YK AS decimal(12,2))) AS ZDYL, MIN(cast(YK AS decimal(12,2))) AS ZDKS, round(avg((NVL(zzc,0)-NVL(zjye,0))/IF(NVL(zzc, 0) = 0, 1, zzc)), 4) AS CW_AVG, AVG(NVL(ZZC,0)) AS ZZC_AVG
                FROM " || F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH) || " GROUP BY KHH";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        --交易频率
        BEGIN
             l_tableName := F_IDS_GET_TABLENAME('investment_JYCS', I_KHH);
             l_sqlBuf := "SELECT KHH, SUM(JYCS) AS JYCS, SUM(NVL(CJJE,0)) AS CJJE  FROM (
                SELECT KHH,
                COUNT(1) AS JYCS ,SUM(CJJE) AS CJJE
                FROM " || F_IDS_GET_TABLENAME('investment_sparkJzjyJgls', I_KHH) || " WHERE JYLB IN (1,2) GROUP BY KHH
                UNION ALL
                SELECT KHH, COUNT(1) AS JYCS , SUM(CJJE) AS CJJE
                FROM " || F_IDS_GET_TABLENAME('investment_sparkXyJgls', I_KHH) || " WHERE JYLB IN (1,2) GROUP BY KHH
                UNION ALL
                SELECT KHH, COUNT(1) AS JYCS , SUM(CJJE) AS CJJE
                FROM " || F_IDS_GET_TABLENAME('investment_sparkQQJgls', I_KHH) || " GROUP BY KHH
                UNION ALL
                SELECT KHH, COUNT(1) AS JYCS , SUM(CASE WHEN QRJE = 0 THEN QRFE ELSE QRJE END) AS CJJE
                FROM " || F_IDS_GET_TABLENAME('investment_sparkFPJgls', I_KHH) || " WHERE YWDM IN ('120','122','124') GROUP BY KHH) GROUP BY KHH";

            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;

        --平均持股天数,持股个数,持股成功数，持股成功率=胜率=CGCGS/CGGS，胜率等级1-5星
        BEGIN
            --计算持股前一日，持股后一日日期
            l_tableName := F_IDS_GET_TABLENAME('investment_CGTS_TEMP', I_KHH);
            l_sqlBuf:= "select
               khh,
               T.rq,
               T.jys,
               T.zqdm,
               f_get_jyr_date(T.rq,-1) as qyr,
               f_get_jyr_date(T.rq,1) as hyr,
               lag(T.rq,1) over(partition by khh,T.jys,T.zqdm order by T.rq) as ccqyr,
               lead(T.rq,1) over(partition by khh,T.jys,T.zqdm order by T.rq) as cchyr,
               dryk,
               T.zqpz,
               (CASE WHEN A.ZQDM IS NOT NULL THEN 1 ELSE 0 END) AS GPZT --股票涨停
             from "|| F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH) || " T LEFT JOIN " || F_IDS_GET_TABLENAME('investment_ZQHQ_ZT', I_KHH) ||
             " A ON (T.JYS = A.JYS AND T.ZQDM = A.ZQDM AND T.RQ = A.RQ)";
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

        --根据持股前一日，持股后一日统计单只股票持股天数
        l_tableName := F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH);
        l_sqlBuf:= "select
               khh,
               jys,
               zqdm,
               sum(dryk) as ljyk,
               sum(case when (ccqyr=rq) or (cchyr=rq) or (ccqyr=qyr and cchyr=hyr) then 1 else 0 end) as cgts,
               sum(gpzt) as gpztcs,
               ZQPZ
             from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP', I_KHH) ||" group by khh,jys,zqdm,ZQPZ";
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);

        --汇总持股天数并求平均值,持股个数/持股成功率,股票涨停次数
        l_tableName := F_IDS_GET_TABLENAME('investment_CGTS', I_KHH);
        l_sqlBuf:= "select
               khh,
               sum(cgts)/count(1) as CGTS_AVG,
               sum(case when ljyk > 0 then 1 else 0 end) as CGCGS,
               count(1) as CGGS,
               sum(gpztcs) as gpztcs
             from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) ||" group by khh";
        F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;


         --证券偏好
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_zqpz', I_KHH);
            l_sqlBuf:= "select
               khh,
               zqpz,
               zsz
             from
            (select
               khh,
               zqpz,
               zsz,
               row_number() over(partition by khh order by zsz desc) rn
             from
            (select
               khh,
               zqpz,
               sum(zxsz) as zsz
             from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH) ||" group by khh,zqpz)a)a where rn=1";
             F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
        END;
        --股票偏好
        BEGIN
            --股票偏好-行业偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH_HYPH', I_KHH);
                l_sqlBuf:= "select
                       KHH,
                       SSHY AS GPPH_HYPH
                     from
                    (select
                       khh,
                       sshy,
                       zsz,
                       row_number() over(partition by khh order by zsz desc) rn
                     from
                    (select
                       khh,
                       sshy,
                       sum(zxsz) zsz
                     from "|| F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH) || " where zqpz='股票' group by khh,sshy)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --股票偏好-个股偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH_GGPH', I_KHH);
                l_sqlBuf:= "select
                   khh,
                   jys,
                   zqdm,
                   zqmc AS GPPH_GGPH,
                   zsz
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   zsz,
                   row_number() over(partition by khh order by zsz desc) rn
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   sum(zxsz) as zsz
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" where zqpz='股票' group by khh,jys,zqdm,zqmc)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --股票偏好-大小盘股
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH_DXP', I_KHH);
                l_sqlBuf:= "select
                       KHH,
                       DXP AS GPPH_DXP
                     from
                    (select
                       khh,
                       DXP,
                       zsz,
                       row_number() over(partition by khh order by zsz desc) rn
                     from
                    (select
                       khh,
                       A.DXP,
                       sum(zxsz) zsz
                     from "|| F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH) || " T LEFT JOIN " || F_IDS_GET_TABLENAME('investment_GPFL', I_KHH) || " A ON (T.ZQDM = A.ZQDM) where zqpz='股票' group by khh,A.DXP)a)a where rn=1";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --股票偏好-高低价
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH_GDJ', I_KHH);
                l_sqlBuf:= "select
                       KHH,
                       GDJ AS GPPH_GDJ
                     from
                    (select
                       khh,
                       GDJ,
                       zsz,
                       row_number() over(partition by khh order by zsz desc) rn
                     from
                    (select
                       khh,
                       A.GDJ,
                       sum(zxsz) zsz
                     from "|| F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH) || " T LEFT JOIN " || F_IDS_GET_TABLENAME('investment_GPFL', I_KHH) || " A ON (T.ZQDM = A.ZQDM) where zqpz='股票' group by khh,A.GDJ)a)a where rn=1";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --股票偏好-持股期限
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH_CGTS', I_KHH);
                l_sqlBuf := "SELECT  khh,
                    cast(case when CGTS_AVG > 14 then
                    '长期持有'
                    when CGTS_AVG < 7 then
                    '短期持有'
                    else
                    '中期持有'
                    end as string) as GPPH_CGTS FROM (select
                   khh,
                   sum(cgts)/count(1) as CGTS_AVG
                 from  "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) || " WHERE ZQPZ = '股票' group by khh)";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --股票偏好汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_GPPH', I_KHH);
                l_sqlBuf := "SELECT T.KHH,
                    CONCAT_WS(',', A.GPPH_HYPH, B.GPPH_GGPH, C.GPPH_DXP, D.GPPH_GDJ, E.GPPH_CGTS) AS GPPH FROM
                    DSC_BAS.T_KHXX_JJYW T LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_GPPH_HYPH', I_KHH) || " A ON (A.KHH = T.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_GPPH_GGPH', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_GPPH_DXP', I_KHH) || " C ON (T.KHH = C.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_GPPH_GDJ', I_KHH) || " D ON (T.KHH = D.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_GPPH_CGTS', I_KHH) || " E ON (T.KHH = E.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;
        --基金偏好
        BEGIN
            --基金偏好-分红方式
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JJPH_FHFS', I_KHH);
                l_sqlBuf := "SELECT KHH,
                    CAST(CASE WHEN HLZT > XJFH THEN
                    '红利转投'
                    ELSE
                    '现金分红'
                    END AS STRING) AS JJPH_FHFS
                FROM (
                    SELECT KHH,
                        SUM(CASE WHEN FHFS = 1 THEN 1 ELSE 0 END) HLZT,
                        SUM(CASE WHEN FHFS = 2 THEN 1 ELSE 0 END) XJFH
                    FROM (
                        SELECT
                            DISTINCT KHH,CPDM,FHFS
                        FROM DSC_BAS.T_FP_CPFE_HIS
                        WHERE RQ BETWEEN " || l_ksrq || " and " || l_jsrq || "
                        )
                GROUP BY KHH
                )";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END
            --基金偏好-产品偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JJPH_CPPH', I_KHH);
                l_sqlBuf:= "select
                   khh,
                   jys,
                   zqdm,
                   zqmc AS JJPH_CPPH,
                   zsz
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   zsz,
                   row_number() over(partition by khh order by zsz desc) rn
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   sum(zxsz) as zsz
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" where zqpz='基金' group by khh,jys,zqdm,zqmc)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --基金偏好-类型偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JJPH_LXPH', I_KHH);
                l_sqlBuf := "SELECT KHH,
                T.ZQPZ,
                A.ZQPZ_NAME AS JJPH_LXPH
                FROM (
                    SELECT KHH,
                        ZQPZ,
                        row_number() over(partition by khh order by LXPH desc) rn
                    FROM (
                        SELECT
                             KHH,ZQPZ,COUNT(1) AS LXPH
                        FROM DSC_BAS.T_FP_CPFE_HIS
                        WHERE RQ BETWEEN " || l_ksrq || " and " || l_jsrq || " GROUP BY KHH,ZQPZ
                        )
                ) T LEFT JOIN DSC_CFG.T_ZQPZ A ON (T.ZQPZ = A.ZQPZ) WHERE T.RN = 1";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --基金偏好-持有期限
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JJPH_CYQX', I_KHH);
                l_sqlBuf:= "SELECT  khh,
                    cast((case when CGTS_AVG > 14 then
                    '长期持有'
                    when CGTS_AVG < 7 then
                    '短期持有'
                    else
                    '中期持有'
                    END) as string) as JJPH_CYQX FROM (select
                   khh,
                   sum(cgts)/count(1) as CGTS_AVG
                 from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) ||" WHERE ZQPZ = '基金' group by khh)";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --基金偏好汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JJPH', I_KHH);
                l_sqlBuf := "SELECT T.KHH,
                    CONCAT_WS(',', A.JJPH_FHFS, B.JJPH_CPPH, C.JJPH_LXPH, D.JJPH_CYQX) AS JJPH FROM
                    DSC_BAS.T_KHXX_JJYW T LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_JJPH_FHFS', I_KHH) || " A ON (A.KHH = T.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_JJPH_CPPH', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_JJPH_LXPH', I_KHH) || " C ON (T.KHH = C.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_JJPH_CYQX', I_KHH) || " D ON (T.KHH = D.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;
        --债券偏好
        BEGIN
            --债券偏好-持有期限
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_ZQPH_CYQX', I_KHH);
                l_sqlBuf:= "SELECT  khh,
                    cast(case when cgts_avg > 14 then
                    '长期持有'
                    when cgts_avg < 7 then
                    '短期持有'
                    else
                    '中期持有'
                    end as string) as ZQPH_CYQX FROM (select
                   khh,
                   sum(cgts)/count(1) as CGTS_AVG
                 from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) ||" WHERE ZQPZ = '债券' group by khh)";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --债券偏好-债券类型
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_ZQPH_ZQLX', I_KHH);
                l_sqlBuf := "SELECT KHH,
                ZQLBMC AS ZQPH_ZQLX
                FROM (
                    SELECT KHH,
                        ZQLBMC,
                        row_number() over(partition by khh order by ZQLX desc) rn
                    FROM (
                        SELECT
                             KHH,ZQLBMC,COUNT(1) AS ZQLX
                        FROM "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" T where zqpz='债券'
                         GROUP BY KHH,ZQLBMC
                        )
                ) T  WHERE T.RN = 1";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --债券偏好-产品偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_ZQPH_CPPH', I_KHH);
                l_sqlBuf:= "select
                   khh,
                   jys,
                   zqdm,
                   zqmc AS ZQPH_CPPH,
                   zsz
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   zsz,
                   row_number() over(partition by khh order by zsz desc) rn
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   sum(zxsz) as zsz
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" where zqpz='债券' group by khh,jys,zqdm,zqmc)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            --债券偏好汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_ZQPH', I_KHH);
                l_sqlBuf := "SELECT T.KHH,
                    CONCAT_WS(',', B.ZQPH_CPPH, C.ZQPH_ZQLX, D.ZQPH_CYQX) AS ZQPH FROM
                    DSC_BAS.T_KHXX_JJYW T LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_ZQPH_CPPH', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_ZQPH_ZQLX', I_KHH) || " C ON (T.KHH = C.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_ZQPH_CYQX', I_KHH) || " D ON (T.KHH = D.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;
        --期权偏好
        BEGIN
            --期权偏好-持有期限
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_QQPH_CYQX', I_KHH);
                l_sqlBuf:= "SELECT  khh,
                    cast(case when cgts_avg > 14 then
                    '长期持有'
                    when cgts_avg < 7 then
                    '短期持有'
                    else
                    '中期持有'
                    end as string) as QQPH_CYQX FROM (select
                   khh,
                   sum(cgts)/count(1) as CGTS_AVG
                 from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) ||" WHERE ZQPZ = '期权' group by khh)";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --期权偏好-产品偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_QQPH_CPPH', I_KHH);
                l_sqlBuf:= "select
                   khh,
                   jys,
                   zqdm,
                   zqmc AS QQPH_CPPH,
                   zsz
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   zsz,
                   row_number() over(partition by khh order by zsz desc) rn
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   sum(zxsz) as zsz
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" where zqpz='期权' group by khh,jys,zqdm,zqmc)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --期权偏好汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_QQPH', I_KHH);
                l_sqlBuf := "SELECT T.KHH,
                    CONCAT_WS(',', B.QQPH_CPPH, D.QQPH_CYQX) AS QQPH FROM
                    DSC_BAS.T_KHXX_JJYW T LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_QQPH_CPPH', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_QQPH_CYQX', I_KHH) || " D ON (T.KHH = D.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;
        --理财偏好
        BEGIN
            --理财偏好-持有期限
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_LCPH_CYQX', I_KHH);
                l_sqlBuf:= "SELECT  khh,
                    cast(case when cgts_avg > 14 then
                    '长期持有'
                    when cgts_avg < 7 then
                    '短期持有'
                    else
                    '中期持有'
                    end as string) as LCPH_CYQX FROM (select
                   khh,
                   sum(cgts)/count(1) as CGTS_AVG
                 from "|| F_IDS_GET_TABLENAME('investment_CGTS_TEMP2', I_KHH) ||" WHERE ZQPZ = '理财' group by khh)";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --理财偏好-产品偏好
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_LCPH_CPPH', I_KHH);
                l_sqlBuf:= "select
                   khh,
                   jys,
                   zqdm,
                   zqmc AS LCPH_CPPH,
                   zsz
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   zsz,
                   row_number() over(partition by khh order by zsz desc) rn
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   zqmc,
                   sum(zxsz) as zsz
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||" where zqpz='理财' group by khh,jys,zqdm,zqmc)a)a where rn=1";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;

            --理财偏好汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_LCPH', I_KHH);
                l_sqlBuf := "SELECT T.KHH,
                    CONCAT_WS(',', B.LCPH_CPPH, D.LCPH_CYQX) AS LCPH FROM
                    DSC_BAS.T_KHXX_JJYW T LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_LCPH_CPPH', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                    "LEFT JOIN " ||
                    F_IDS_GET_TABLENAME('investment_LCPH_CYQX', I_KHH) || " D ON (T.KHH = D.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;


        --盘面感知
        BEGIN
            l_sqlBuf:="SELECT
                khh,
                CAST(cast(cume_dist() OVER (ORDER BY sum(pmgz))*100*0.5+50 AS DECIMAL(10,2)) AS STRING) pmgz
            FROM
                (
                SELECT
                    aa.khh,
                    nvl(aa.mr,0)-nvl(bb.mc,0)pmgz
                FROM
                    (
                    SELECT
                        khh,
                        sum(mr)mr
                    FROM
                        (
                        SELECT khh,
                        SUM((a.zxj-b.zsp)/ b.zsp-(cjjg-b.zsp)/ b.zsp)mr
                        from
                        " || F_IDS_GET_TABLENAME('investment_sparkJzjyJgls',I_KHH) || " a
                    JOIN dsc_bas.t_zqhq_xz_his b ON
                        a.`zqdm` = b.zqdm
                        AND a.jys = b.jys
                        AND a.cjrq = f_get_jyr_date(b.rq,
                        1)
                    WHERE
                        jylb IN ('1',
                        '59')
                    GROUP BY
                        khh
                UNION ALL
                    SELECT
                        khh,
                        SUM((a.zxj-b.zsp)/ b.zsp-(cjjg-b.zsp)/ b.zsp)mr
                    FROM
                        " || F_IDS_GET_TABLENAME('investment_sparkXyJgls',I_KHH) || " a
                    JOIN dsc_bas.t_zqhq_xz_his b ON
                        a.`zqdm` = b.zqdm
                        AND a.jys = b.jys
                        AND a.cjrq = f_get_jyr_date( b.rq,
                        1)
                    WHERE
                        jylb IN ('61','1')
                    GROUP BY
                        khh )a
                GROUP BY
                    khh ) aa

            LEFT JOIN (
                SELECT
                    khh,
                    SUM(mc) mc
                FROM
                    (
                    SELECT
                        khh,
                        SUM((a.zxj-b.zsp)/ b.zsp-(cjjg-b.zsp)/ b.zsp)mc
                    FROM
                        " || F_IDS_GET_TABLENAME('investment_sparkJzjyJgls',I_KHH) || " a
                    JOIN dsc_bas.t_zqhq_xz_his b ON
                        a.`zqdm` = b.zqdm
                        AND a.jys = b.jys
                        AND a.cjrq = f_get_jyr_date(b.rq,
                        1)
                    WHERE
                        jylb IN ('2')
                    GROUP BY
                        khh
                UNION ALL
                    SELECT
                        khh,
                        SUM((a.zxj-b.zsp)/ b.zsp-(cjjg-b.zsp)/ b.zsp)mc
                    FROM
                        " || F_IDS_GET_TABLENAME('investment_sparkXyJgls',I_KHH) || " a
                    JOIN dsc_bas.t_zqhq_xz_his b ON
                        a.`zqdm` = b.zqdm
                        AND a.jys = b.jys
                        AND a.cjrq = f_get_jyr_date(b.rq,
                        1)
                    WHERE
                        jylb IN ('62','2')
                    GROUP BY
                        khh )b
                GROUP BY
                    khh ) bb ON
                aa.khh = bb.khh)c GROUP BY khh ORDER BY pmgz DESC";
            l_tableName:=F_IDS_GET_TABLENAME('investment_sparkpmgz',I_KHH);
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        END;


        --选股能力
        BEGIN
            BEGIN
                l_tableName:=F_IDS_GET_TABLENAME('investment_sparkxgnl',I_KHH);
                l_sqlBuf:= "select
                   khh,
                   if(cggs=0,0,cgcgs/cggs) * 100 as xgnl
                 from
                (select
                   khh,
                   sum(case when ljyk > 0 then 1 else 0 end) as cgcgs,
                   count(distinct concat(jys,zqdm)) as cggs
                 from
                (select
                   khh,
                   jys,
                   zqdm,
                   sum(dryk) as ljyk
                 from "||F_IDS_GET_TABLENAME('investment_sparkTzfb', I_KHH)||"  group by khh,jys,zqdm)a group by khh)a";
                 F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
        END;



        --风险控制
        BEGIN
            l_tableName := F_IDS_GET_TABLENAME('investment_FXKZ', I_KHH);
            l_sqlBuf := "
                SELECT A.KHH,CUST.F_IDS_GET_FKNL(A.SHARP, B.ZDHCL) AS FXKZ FROM
                " || F_IDS_GET_TABLENAME('investment_sparkZcfx',I_KHH) || " B
                 LEFT JOIN  " || F_IDS_GET_TABLENAME('investment_sharp',I_KHH) || " A
                ON (A.KHH = B.KHH)
            ";
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        END
        --收益能力
        BEGIN

            l_sqlBuf:="
                SELECT KHH,CUST.F_IDS_GET_SYNL(YKL,B.ZSZZL) AS SYNL
            FROM
                " || F_IDS_GET_TABLENAME('investment_sparkZcfx', I_KHH) || " A
                ,"|| F_IDS_GET_TABLENAME('investment_ZSZZL', I_KHH) ||" B
            ";
            l_tableName:=F_IDS_GET_TABLENAME('investment_SYNL',I_KHH);
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);

        -- BEGIN
            -- l_sqlBuf:="
                -- SELECT KHH,SUM(YKL-ZSHQ_HS300_ZZL) AS SYNL
            -- FROM
                -- " || (F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH)) || "
            -- GROUP BY
                -- khh
            -- ";
            -- l_tableName:=F_IDS_GET_TABLENAME('investment_SYNL',I_KHH);
            -- F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        -- END;
        -- BEGIN
            -- l_tableName:=F_IDS_GET_TABLENAME("sparkfxkzAndYlnl",I_KHH);
            -- l_sqlBuf:="SELECT a.khh,a.fxkz fxkz,b.synl synl
                    -- FROM
                   -- " || F_IDS_GET_TABLENAME('investment_FXKZ',I_KHH) || " A
                    -- left join
                     -- " || F_IDS_GET_TABLENAME('investment_SYNL',I_KHH) || " B
                    -- ON a.khh=b.khh";
            -- F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        -- END;
        END;
         --仓位控制
        BEGIN
            l_sqlBuf:="SELECT
                khh,
                CAST(CUME_DIST() OVER(ORDER BY CAST(zbz AS DOUBLE) ASC)* 100*0.5+50 AS DECIMAL(10,2)) AS cknl,
                order_id,
                zbmc
            FROM
                (
                SELECT
                    f_ids_get_cwkz(ROW)
                FROM
                    (
                        SELECT khh,
                        groupRow(khh,
                        rq,
                        zqsz_jzjy,
                        zqsz_rzrq,
                        zshq_hs300) AS ROW
                    FROM
                        " || (F_IDS_GET_TABLENAME('investment_sparkRzd', I_KHH)) || "
                    GROUP BY
                        khh)a)aa";
            l_tableName:=F_IDS_GET_TABLENAME("investment_sparkCwkz",I_KHH);
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        END;


        --择时能力
        BEGIN
            l_sqlBuf:="select khh,CUST.F_IDS_GET_ZSNL(zscgl,ztcgl_gp,czcs,ztcs_gp) as zsnl ,czcs,ztcs_gp,ztcgl_gp,zscgl
                from (select
                       khh,
                       sum(czcs) as czcs,
                       sum(case when mrsl>0 and mcsl>0 then 1 else 0 end) as ztcs_gp,  --股票做T次数
                       sum(case when mrsl>0 and mcsl>0 then if((mcje/mcsl)>(mrje/mrsl),1,0) else 0 end) /
                       sum(case when  mrsl>0 and mcsl>0 then 1 else 0 end) as ztcgl_gp,  --股票做T成功次数
                       sum(jycg)/sum(czcs) as zscgl  --交易成功率
                     from (select
                       khh,
                       jys,
                       zqdm,
                       lb,
                       zqpzmc,
                       sshy,
                       sum(case when jylb='1' then 1 else 0 end) as mrcs,
                       sum(case when jylb='2' then 1 else 0 end) as mccs,
                       sum(case when jylb='1' then abs(ysje) else 0 end) as mrje,
                       sum(case when jylb='2' then abs(ysje) else 0 end) as mcje,
                       sum(case when jylb='1' then abs(yssl) else 0 end) as mrsl,
                       sum(case when jylb='2' then abs(yssl) else 0 end) as mcsl,
                       sum(case when (jylb='1' and cjjg<zxj) or (jylb='2' and cjjg>zxj) then 1 else 0 end) jycg,  --交易成功次数
                       count(1) as czcs,
                       sum(abs(ysje)) as jyl,  --交易量
                       cjrq
                     from (select z.khh,z.cjrq,z.jys,z.zqdm,z.zqlb,z.jylb,z.ysje,z.yssl,z.zxj,z.cjjg,z.lb,a.zqpzmc,regexp_replace(b.sshy,'[,; ]','') as sshy
                     from (select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,zxj,cjjg,'jzjy' as lb from " || F_IDS_GET_TABLENAME('investment_sparkJzjyJgls',I_KHH) || "
                union all
                select khh,cjrq,jys,zqdm,zqlb,jylb,ysje,yssl,zxj,cjjg,'rzrq' as lb from " || F_IDS_GET_TABLENAME('investment_sparkXyJgls',I_KHH) || "
                )z
                     left join (select * from DSC_CFG.VW_T_ZQLB_IDS) a on (z.zqlb=a.zqlb and z.jys=a.jys)
                     left join
                     (select jys,zqdm,sshy from (select *,row_number() over(partition by jys,zqdm order by id desc)
                     rn from info.tgp_gsgk where length(sshy)>0 and sshy!='null' and sshy!='无') where rn=1) b
                     on (z.jys=b.jys and z.zqdm=b.zqdm))
                     group by khh,jys,zqdm,lb,cjrq,zqpzmc,sshy)
                     group by khh
                     )";
            l_tableName:=F_IDS_GET_TABLENAME("investment_sparkZsnl",I_KHH);
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
        END;



        --整合择时能力，选股能力，仓位控制，盘面管制，风险控制与收益能力
        BEGIN
            l_tableName:=F_IDS_GET_TABLENAME("investment_sparkTzzd",I_KHH);
            l_sqlBuf := "
                select A.KHH,
                       CAST(NVL(A.FXKZ,50) AS DECIMAL(16,2)) AS FKNL,
                       CAST(NVL(B.XGNL,50) AS DECIMAL(16,2)) AS XGNL,
                       CAST(NVL(C.SYNL,50) AS DECIMAL(16,2)) AS YLNL,
                       CAST(NVL(D.CKNL,50) AS DECIMAL(16,2)) AS CKNL,
                       CAST(NVL(E.PMGZ,50) AS DECIMAL(16,2)) AS PMGZ,
                       CAST(NVL(F.ZSNL,50) AS DECIMAL(16,2)) AS ZSNL,
                       CAST((CAST(NVL(A.FXKZ,50) AS DECIMAL(16,2)) + CAST(NVL(B.XGNL,50) AS DECIMAL(16,2)) + CAST(NVL(C.SYNL,50) AS DECIMAL(16,2)) +
                       CAST(NVL(D.CKNL,50) AS DECIMAL(16,2)) + CAST(NVL(E.PMGZ,50) AS DECIMAL(16,2)) + CAST(NVL(F.ZSNL,50) AS DECIMAL(16,2))) AS DECIMAL(16,2)) AS ZHPF
                from "|| F_IDS_GET_TABLENAME('investment_FXKZ', I_KHH) || " A
                LEFT JOIN "|| F_IDS_GET_TABLENAME('investment_sparkxgnl', I_KHH) || " B
                on (A.KHH = B.KHH)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('investment_SYNL', I_KHH) || " C
                ON (A.KHH = C.KHH)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('investment_sparkCwkz', I_KHH) || " D
                ON (A.KHH = D.KHH)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('investment_sparkpmgz', I_KHH) || " E
                ON (A.KHH = E.KHH)
                LEFT JOIN "|| F_IDS_GET_TABLENAME('investment_sparkZsnl', I_KHH) || " F
                ON (A.KHH = F.KHH)
                ";
            F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);

            -- l_sqlBuf:="
                        -- SELECT final2.khh KHH,
                               -- final1.pmgz PMGZ,
                                -- final1.xgnl XGNL,
                                -- final2.zsnl ZSNL,
                                -- final1.synl YLNL,
                                -- final1.zbz CKNL,
                                -- final1.fxkz FKNL,
                                -- final1.pmgz+final1.xgnl+final2.zsnl+final1.synl+final1.zbz+final1.fxkz ZHPF
                        -- from(
                            -- SELECT third2.khh khh,third2.xgnl xgnl,third1.zbz zbz,third1.pmgz pmgz,third1.fxkz fxkz,
                            -- third1.synl synl
                            -- FROM(
                               -- SELECT second2.khh khh,second2.zbz zbz,second1.pmgz pmgz,
                                -- second1.fxkz fxkz,second1.synl synl
                               -- FROM (
                                    -- SELECT p.khh khh,p.pmgz pmgz,s.fxkz fxkz,s.synl synl
                                    -- FROM " || (F_IDS_GET_TABLENAME('sparkpmgz', I_KHH)) || " p
                                    -- LEFT JOIN
                                    -- " || (F_IDS_GET_TABLENAME("sparkfxkzAndYlnl", I_KHH)) || " s
                                    -- ON s.khh=p.khh
                                    -- ) second1
                                -- LEFT JOIN " || (F_IDS_GET_TABLENAME("sparkCwkz",I_KHH)) || " second2 ON second2.khh=second1.khh
                             -- ) third1
                             -- LEFT JOIN " || (F_IDS_GET_TABLENAME('investment_sparkxgnl',I_KHH)) || " third2 ON third2.khh=third1.khh
                        -- ) final1 LEFT JOIN " || (F_IDS_GET_TABLENAME("sparkZsnl",I_KHH)) || " final2 ON final2.khh=final1.khh
                        -- ";
            -- l_tableName:=F_IDS_GET_TABLENAME("sparkZsnlAndPmgzAndSynlAndCwglAndxgnl",I_KHH);
            -- F_IDS_CREATE_TEMP_TABLE(l_sqlBuf,l_tableName);
         END;




        BEGIN
            --交易风格汇总
            BEGIN
                l_tableName := F_IDS_GET_TABLENAME('investment_JYFG', I_KHH);
                l_sqlBuf := "SELECT KHH, (HSL_PF + CGTS_PF + JYPL_PF + PJCW_PF + FXPH_PF) AS JYFG_PF
                FROM
                   (SELECT T.KHH,
                            (CASE WHEN NVL(B.CJJE/C.ZZC_AVG,0) = 0 THEN 0 WHEN NVL(B.CJJE/C.ZZC_AVG,0) < 0.03 THEN 0.5 WHEN NVL(B.CJJE/C.ZZC_AVG,0) < 0.15 THEN 1 ELSE 2 END) AS HSL_PF,
                            (case when NVL(A.cgts_avg,0) = 0 then 0 when NVL(A.cgts_avg,0) > 14 then 0.5 when NVL(A.cgts_avg,0) < 7 then  2 else 1 end)
                            AS CGTS_PF,
                            (CASE WHEN NVL(B.JYCS,0) = 0 THEN  0   WHEN B.JYCS/T.BYTS<0.25 THEN  0.5 WHEN B.JYCS/T.BYTS<0.7 THEN  1 ELSE 2 END)
                            AS JYPL_PF,
                            (case when nvl(C.CW_AVG,0) = 0 THEN 0 WHEN C.CW_AVG < 0.3 THEN 0.5 WHEN C.CW_AVG < 0.6 THEN 1 ELSE 2 END)
                            AS PJCW_PF,
                            (case when NVL(PZ.ZQPZ,0) = 0 THEN 0 WHEN PZ.zqpz = '股票' THEN 2 WHEN PZ.ZQPZ='基金' THEN 1 else 0.5 end)
                            AS FXPH_PF
                            FROM " || F_IDS_GET_TABLENAME('investment_sparkZCFX', I_KHH) ||  --最大回撤
                            " T LEFT JOIN " ||
                            F_IDS_GET_TABLENAME('investment_CGTS', I_KHH) || " A ON (T.KHH = A.KHH) " || --汇总持股天数并求平均值,持股个数/持股成功率,股票涨停次数
                            " LEFT JOIN " ||
                            F_IDS_GET_TABLENAME('investment_JYCS', I_KHH) || " B ON (T.KHH = B.KHH) " || --交易次数
                            " LEFT JOIN " ||
                            F_IDS_GET_TABLENAME('investment_CW', I_KHH)   || " C ON (T.KHH = C.KHH) " || --平均仓位,最大盈亏，最大亏损
                            " LEFT JOIN " ||
                            F_IDS_GET_TABLENAME('investment_zqpz', I_KHH) || " PZ ON (T.KHH = PZ.KHH))"; --证券偏好
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName);
            END;
            -- 创建最终结果表
            l_tableName := F_IDS_GET_TABLENAME('investment_SparkTznl', I_KHH);
            --资产分析
            l_sqlBuf := "SELECT" ||
                      "   T.KHH," ||
                      "   NVL(E.YLNL, 50) AS YLNL," ||
                      "   NVL(E.PMGZ, 50) AS PMGZ," ||
                      "   NVL(E.XGNL, 50) AS XGNL," ||
                      "   NVL(E.ZSNL, 50) AS ZSNL," ||
                      "   NVL(E.CKNL, 50) AS CKNL," ||
                      "   NVL(E.FKNL, 50) AS FKNL," ||
                      "   NVL(E.ZHPF, 300) AS ZHPF," ||
                       "  CAST(case when NVL(FG.JYFG_PF,0) < 2 then '保守型'
                      WHEN FG.JYFG_PF < 4 THEN
                      '谨慎型'
                      WHEN FG.JYFG_PF < 6 THEN
                      '稳健型'
                      WHEN FG.JYFG_PF < 8 THEN
                      '积极型'
                      ELSE
                      '激进型'
                      END AS STRING) AS JYFG," ||
                      "   CAST(BDL AS DECIMAL(12,4)) AS BDL," ||
                      "   CAST(T.ZDHCL AS DECIMAL(12,4)) AS ZDHCL," ||
                      "   CAST(D.SHARP AS DECIMAL(12,4)) AS SHARP," ||
                      "   NVL(A.CGGS,0) AS CGGS," ||
                      "   NVL(A.GPZTCS,0) AS CGZTCS," ||
                      "   CAST(IF(nvl(C.ZDYL,0) < 0, 0, C.ZDYL) AS DECIMAL(16,2)) AS ZDYL," ||
                      "   CAST(IF(NVL(C.ZDKS,0) > 0, 0, C.ZDKS) AS DECIMAL(16,2)) AS ZDKS," ||
                      "   CAST(IF(NVL(A.CGGS, 0) = 0, 1, (A.CGCGS / A.CGGS)) AS DECIMAL(12,4)) AS CGSL_PF," ||
                      "   CAST(CASE
                              WHEN  IF(NVL(A.CGGS, 0) = 0, 1, (A.CGCGS / A.CGGS)) >= 0.9 THEN
                              '五星'
                              WHEN  IF(NVL(A.CGGS, 0) = 0, 1, (A.CGCGS / A.CGGS)) >= 0.6 THEN
                              '四星'
                              WHEN  IF(NVL(A.CGGS, 0) = 0, 1, (A.CGCGS / A.CGGS)) >= 0.35 THEN
                              '三星'
                              WHEN  IF(NVL(A.CGGS, 0) = 0, 1, (A.CGCGS / A.CGGS)) >= 0.15 THEN
                              '二星'
                              ELSE
                              '一星'
                      END AS STRING) AS CGSL_DJ," ||
                      "   CAST(C.CW_AVG AS DECIMAL(12,4)) AS CW_AVG," ||
                      "   ROUND(A.CGTS_AVG) AS CGTS_AVG," ||
                      "   nvl(B.JYCS,0) AS JYCS," ||
                      " CAST(CASE WHEN B.JYCS IS NULL THEN
                        '低频'
                        WHEN B.JYCS/T.BYTS<0.25 THEN
                        '低频'
                        WHEN B.JYCS/T.BYTS<=0.7 THEN
                        '中频'
                        ELSE
                        '高频'
                        END AS STRING) AS JYPL," ||
                      "  concat('近一个月共买卖', NVL(B.JYCS,0), '次，交易股票', NVL(A.CGGS,0), '只，抓住',
                      NVL(A.GPZTCS,0), '次涨停，最大盈利',NVL(C.ZDYL,0), '元，最大亏损', NVL(C.ZDKS,0), '元，夏普比率是', ROUND(NVL(D.SHARP,0),2), '，最大回撤是',
                      ROUND(NVL(T.ZDHCL * 100,0),2), '%。') AS ZY," ||
                      "   CAST(LC.LCPH AS STRING) AS LCPH," ||
                      "   CAST(GP.GPPH AS STRING) AS GPPH," ||
                      "   CAST(JJ.JJPH AS STRING) AS JJPH," ||
                      "   CAST(ZQ.ZQPH AS STRING) AS ZQPH," ||
                      "   CAST(QQ.QQPH AS STRING) AS QQPH, " ||
                      I_RQ || " AS RQ " ||
                      "   FROM  " ||
                      F_IDS_GET_TABLENAME('investment_sparkZCFX', I_KHH) ||
                      " T LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_CGTS', I_KHH) || " A ON (T.KHH = A.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_JYCS', I_KHH) || " B ON (T.KHH = B.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_CW', I_KHH)   || " C ON (T.KHH = C.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_sharp', I_KHH) || " D ON (T.KHH = D.KHH)" ||
                      "LEFT JOIN "  ||
                      F_IDS_GET_TABLENAME("investment_sparkTzzd",I_KHH) || " E ON (T.KHH=E.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_GPPH', I_KHH) || " GP ON (T.KHH = GP.KHH) " ||
                       " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_JJPH', I_KHH) || " JJ ON (T.KHH = JJ.KHH) " ||
                       " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_ZQPH', I_KHH) || " ZQ ON (T.KHH = ZQ.KHH) " ||
                       " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_QQPH', I_KHH) || " QQ ON (T.KHH = QQ.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_LCPH', I_KHH) || " LC ON (T.KHH = LC.KHH) " ||
                      " LEFT JOIN " ||
                      F_IDS_GET_TABLENAME('investment_JYFG', I_KHH) || " FG ON (T.KHH = FG.KHH) ";
                F_IDS_CREATE_TEMP_TABLE(l_sqlBuf, l_tableName)
        END;

    --数据写入分区表
    BEGIN
        l_tableName := 'T_STAT_KHFX_TZNL'
        F_IDS_OVERWRITE_PARTITION(F_IDS_GET_TABLENAME('investment_SparkTznl', I_KHH), "CUST", l_tableName , I_RQ, I_KHH)
    END

    --数据写入HBase表
    --PMGZ,ZSNL,FKNL
    BEGIN
        INSERT INTO APEX.KHFX_TZNL
            SELECT /*+USE_BULKLOAD*/
                concat_ws('-',reverse(KHH), cast(RQ AS STRING)) AS rowkey,
                KHH,
                RQ,
                ZHPF,
                PMGZ,
                XGNL,
                ZSNL,
                YLNL,
                CKNL,
                FKNL,
                JYFG,
                ZDHCL,
                SHARP,
                BDL,
                CGGS,
                CGZTCS,
                ZDYL,
                ZDKS,
                CGSL_PF,
                CGSL_DJ,
                CW_AVG,
                CGTS_AVG,
                JYCS,
                JYPL,
                ZY,
                LCPH,
                GPPH,
                JJPH,
                ZQPH,
                QQPH
            FROM CUST.T_STAT_KHFX_TZNL A WHERE RQ = I_RQ ORDER BY rowkey;
    END;

END;

/******************************************************************
  *文件名称：dm_credit_risk_analysis
  *项目名称：信用风险分析指标
  *文件说明：信用风险分析指标

  创建人：guof
  功能说明：信用风险分析指标

  模型表：
   1.datastudio_dm.dm_xy_tkhxx
   2.datastudio_dm.dm_xy_tkhfzxx
   3.datastudio_dm.dm_xy_tkhccxx

  指标结果表：
   1.datastudio_dm.dm_xy_fxfx4
   2.datastudio_dm.dm_xy_fxfx3_zt
   3.datastudio_dm.dm_xy_fxfx3_mx
   4.datastudio_dm.dm_xy_fxfx2_zt
   5.datastudio_dm.dm_xy_fxfx2_mx
   6.datastudio_dm.dm_xy_fxfx1

  修改者        版本号        修改日期        说明
  guof          V1.0.0       2020/12/05     创建

*******************************************************************/
set  hive mapred.reduce.tasks=50;
set  hive.merge.mapfiles=true;
set  hive.merge.mapredfiles=true;
set  hive.merge.size.per.task=256000000;
set  hive.merge.smallfiles.avgsize=16000000;

/**
*
* 一、写入基础模型表:
*
**/

-- 1.客户信息总表
insert overwrite TABLE datastudio_dm.dm_xy_tkhxx PARTITION(dm_dt='${v_busi_date}')
select '${v_busi_date}' as rq,
       d.jgdm,
       d.jgmc,
       a.khh,
       a.khxm,
       c.zzc    AS zzc,
       b.zhye   as xjye,
       c.fzje   AS zfz,
       c.jzc    AS jzc,
       -- (c.shdbsz + c.szdbsz + c.xgsgsz) AS dbsz,
       (c.zzc - c.zhye) as dbsz,
       c.dbbl
  from datastudio_dsa.ddcrd_tkhxx a,
       datastudio_dsa.ddcrd_tzjzh b,
       datastudio_dsa.ddcrd_txy_htxxls c,
       datastudio_dsa.ddcrd_tjggl d
 where
   a.dsa_dt = CASE WHEN '${v_busi_date}' >= '20201204' THEN '${v_busi_date}' ELSE '20201204' end
   and b.dsa_dt = '${v_busi_date}'
   and c.dsa_pt = '${v_busi_date}'
   and c.rq = '${v_busi_date}'
   and a.yyb = b.yyb
   and a.khh = b.khh
   and a.khh = c.khh
   and a.yyb = c.yyb
   --and a.khzt = '0'
   and b.zhzt = '0'
   and a.yyb = d.jgdm
   and d.dsa_pt = CASE WHEN '${v_busi_date}' >= '20201223' THEN '${v_busi_date}' ELSE '20201223' end;

-- 2.客户持持仓明细表
insert overwrite TABLE datastudio_dm.dm_xy_tkhccxx PARTITION(dm_dt='${v_busi_date}')
select '${v_busi_date}' as rq,t3.jgdm,t3.jgmc,t.khh,t.khxm, t.jys,t.zqdm,t.zqmc,t2.zqlbmc,t.zqsl,t.zxsz
  from datastudio_dsa.ddcrd_tzqgl t
  left join datastudio_dsa.ddcrd_tzqlb t2 on t.zqlb = t2.zqlb and t.jys = t2.jys
  left join datastudio_dsa.ddcrd_tjggl t3 on t.yyb = t3.jgdm and t3.dsa_pt = CASE WHEN '${v_busi_date}' >= '20201223' THEN '${v_busi_date}' ELSE '20201223' end
 where t.dsa_dt = '${v_busi_date}'
;

-- 3.客户合约明细表
insert overwrite TABLE datastudio_dm.dm_xy_tkhfzxx PARTITION(dm_dt='${v_busi_date}')
select '${v_busi_date}' as rq,t3.jgdm,t3.jgmc,t.khh,t.khxm,t.zqdm,t.zqmc,t2.zqlbmc,t.lsh as hybh,t.sgsl,t.zxsz,
         (t.rzje - (t.hkje - t.ghlxfy)
       + (t.rzfy-t.ghfy)
       + (t.yjlx+t.fdlx - (t.ghlx - t.fxghlx))
       + (t.fxyjlx + t.fxfdlx - t.fxghlx)) AS zfz
  from datastudio_dsa.ddcrd_txy_fzxxls t
  left join datastudio_dsa.ddcrd_tzqlb t2 on t.zqlb=t2.zqlb and t.jys=t2.jys
  left join datastudio_dsa.ddcrd_tjggl t3 on t.yyb = t3.jgdm and t3.dsa_pt = CASE WHEN '${v_busi_date}' >= '20201223' THEN '${v_busi_date}' ELSE '20201223' end
 where t.dsa_pt= '${v_busi_date}'
   and t.fzzt <> 3
;


/**
*
* 二、计算信用风险指标:
*
**/
-- 客户级持仓集中度
insert overwrite TABLE datastudio_dm.dm_xy_fxfx1 PARTITION(dm_pt='${v_busi_date}')
SELECT '${v_busi_date}' AS rq,a.yyb,a.yybmc,a.khh,b.khxm,
       a.zqdm,a.zqmc,a.zqlb,a.ccsz,b.zzc,b.dbsz as zsz,b.xjye as xjje,b.dbbl as wcdbbl,b.zfz,b.jzc,
       a.ccsz / b.zzc as jzdbl
  FROM datastudio_dm.dm_xy_tkhccxx a ,
       datastudio_dm.dm_xy_tkhxx b
 where a.yyb = b.yyb
   and a.khh = b.khh
   and a.rq = b.rq
   and a.rq = '${v_busi_date}';


-- 公司级持仓集中度
insert overwrite TABLE datastudio_dm.dm_xy_fxfx2_zt PARTITION(dm_pt='${v_busi_date}')
SELECT x.rq,
       x.zqdm,
       x.zqmc,
       x.zqlb,
       x.dzqsz AS hjsz,
       x.dzqsz / y.dbsz AS dbszbl,
       x.dzqsz / y.zzc AS dbzcbl,
       y.dbsz AS hjzsz,
       y.zzc AS hjzzc,
       x.zrs,
       x.yybjs
  FROM (SELECT min(a.rq) AS rq,
               a.zqdm,
               min(a.zqmc) AS zqmc,
               a.zqlb,
               sum(a.ccsz) AS dzqsz,
               count(DISTINCT khh) AS zrs,
               count(DISTINCT yyb) AS yybjs
          FROM datastudio_dm.dm_xy_tkhccxx a
         WHERE a.rq = '${v_busi_date}'
         GROUP BY a.zqdm,a.zqlb) x,
       (SELECT rq, sum(dbsz) AS dbsz, sum(zzc) AS zzc
          FROM datastudio_dm.dm_xy_tkhxx
         WHERE rq = '${v_busi_date}'
         GROUP BY rq) y
 where x.rq = y.rq;

-- 持仓集中度明细查询
insert overwrite TABLE datastudio_dm.dm_xy_fxfx2_mx PARTITION(dm_pt='${v_busi_date}')
SELECT '${v_busi_date}' AS rq,a.yyb,a.yybmc,a.khh,a.khxm,a.zqdm,a.zqmc,a.zqlb,a.ccsz,a.zzc,a.ccbl as ccszbl,
       a.dbsz as zsz,a.xjye as xjje,a.dbbl as wcdbbl,a.zfz,a.jzc,b.zqs as zqzzs
  FROM (SELECT a.yyb,a.yybmc,a.khh,b.khxm,a.zqdm,a.zqmc,a.zqlb,a.ccsz,b.zzc,
               a.ccsz / b.zzc AS ccbl,
               b.dbsz,b.xjye,b.dbbl,b.zfz,b.jzc
          FROM datastudio_dm.dm_xy_tkhccxx a, datastudio_dm.dm_xy_tkhxx b
         where a.yyb = b.yyb
           AND a.khh = b.khh
           AND a.rq = b.rq
           AND a.rq = '${v_busi_date}') a,
       (SELECT x.yyb, x.khh, count(DISTINCT x.zqdm) AS zqs
          FROM datastudio_dm.dm_xy_tkhccxx x WHERE x.rq = '${v_busi_date}'
         GROUP BY x.yyb, x.khh) b
 WHERE a.yyb = b.yyb
   AND a.khh = b.khh;

-- 单只证券增长幅度
insert overwrite TABLE datastudio_dm.dm_xy_fxfx3_zt PARTITION(dm_pt='${v_busi_date}')
SELECT '${v_busi_date}' AS rq,
       x.zqdm,
       min(x.zqmc) AS zqmc,
       x.zqlb,
       sum(x.ccsz) AS hjsz,
       count(DISTINCT x.khh) AS zrs,
       count(DISTINCT x.yyb) AS yybjs
  FROM datastudio_dm.dm_xy_tkhccxx x
 WHERE rq = '${v_busi_date}'
 GROUP BY x.zqdm,x.zqlb;


-- 负债与持仓不一致
drop table if EXISTS datastudio_dm.dm_xy_fz_cc_intr;
create table datastudio_dm.dm_xy_fz_cc_intr as
SELECT x.yyb,x.yybmc,x.khh,x.khxm,x.zqdm,x.zqmc,x.zqlb,x.fzsl,x.fzsz,nvl(y.ccsl,0) AS ccsl,nvl(y.ccsz,0) AS ccsz
from
(SELECT a.yyb,a.yybmc,
               a.khh,
               min(a.khxm) AS khxm,
               a.zqdm,
               min(a.zqmc) AS zqmc,
               a.zqlb,
               sum(a.ccsl) AS fzsl,
               sum(a.zqsz) AS fzsz
          FROM datastudio_dm.dm_xy_tkhfzxx a
         WHERE a.rq = '${v_busi_date}'
         GROUP BY a.yyb, a.yybmc, a.khh, a.zqdm, a.zqlb) x
       left join
       (SELECT a.yyb,
               a.khh,
               a.zqdm,
               min(a.zqmc) AS zqmc,
               a.zqlb,
               sum(a.ccsl) AS ccsl,
               sum(a.ccsz) AS ccsz
          FROM datastudio_dm.dm_xy_tkhccxx a
         WHERE a.rq = '${v_busi_date}'
         GROUP BY a.yyb, a.khh, a.zqdm ,a.zqlb) y
ON x.yyb = y.yyb
AND x.khh = y.khh
AND x.zqdm = y.zqdm
AND x.zqlb=y.zqlb;


insert overwrite TABLE datastudio_dm.dm_xy_fxfx4 PARTITION(dm_pt='${v_busi_date}')
SELECT '${v_busi_date}' AS rq,
       tt.yyb,
       tt.yybmc,
       tt.khh,
       tt.khxm,
       tt.zqdm,
       tt.zqmc,
       tt.zqlb,
       tt.fzsl AS fzslze,
       tt.ccsl AS ccslze,
       tt.fzsl - tt.ccsl AS fzccce,
       tt.ccsz,
       w.zzc,
       tt.ccsz / w.zzc AS ccszbl,
       w.dbsz AS zqzsz,
       w.xjye AS xjje,
       w.dbbl AS wcdbbl,
       w.zfz,
       w.jzc,
       n.fzbj AS rzmrzje,
       z.bdsz AS bdzqzsz,
       n.fzbj - z.bdsz AS jeszce,
       m.zqs AS zqzzs
  FROM datastudio_dm.dm_xy_fz_cc_intr tt
  left join (SELECT c.yyb, c.khh, sum(c.ccsz) bdsz
               FROM datastudio_dm.dm_xy_tkhccxx c
              WHERE c.dm_dt = '${v_busi_date}'
                and c.zqdm in (SELECT zqdm FROM datastudio_dsa.ddcrd_txy_zgzq WHERE zqsx = '7' and dsa_dt = '${v_busi_date}')
                and c.rq = '${v_busi_date}'
              GROUP BY c.yyb, c.khh) z
    ON tt.yyb = z.yyb
   AND tt.khh = z.khh
  left join (SELECT d.yyb, d.khh, d.zzc, d.dbsz, d.xjye, d.dbbl, d.zfz, d.jzc
               FROM datastudio_dm.dm_xy_tkhxx d
              where d.dm_dt = '${v_busi_date}'
                and d.rq = '${v_busi_date}') w
    ON tt.yyb = w.yyb
   AND tt.khh = w.khh
  left join (SELECT x.yyb, x.khh, count(DISTINCT x.zqdm) AS zqs
               FROM datastudio_dm.dm_xy_tkhccxx x
              WHERE x.dm_dt = '${v_busi_date}'
                and x.rq = '${v_busi_date}'
              GROUP BY x.yyb, x.khh) m
    ON tt.yyb = m.yyb
   AND tt.khh = m.khh
  left join (SELECT a.yyb, a.khh, sum(a.rzje - (a.hkje - a.ghlxfy)) AS fzbj
               FROM datastudio_dsa.ddcrd_txy_fzxxls a
              WHERE a.dsa_pt = '${v_busi_date}'
                and a.fzzt <> 3
              GROUP BY a.yyb, a.khh) n
    ON tt.yyb = n.yyb
   AND tt.khh = n.khh;



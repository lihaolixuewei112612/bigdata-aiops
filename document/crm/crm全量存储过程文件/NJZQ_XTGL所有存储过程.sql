CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_BJQH(
  cur_Result OUT sys_refcursor,
  i_tjrq       in  number       --统计日期
  )
is

/*
  功能说明:百家争先营业部排名-百家千户

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);
v_ksrq  number(8);
v_jsrq  number(8);
v_qxktksrq number(8);
v_qxktjsrq number(8);
v_rqje  number(16);
begin
v_ksrq := substr(i_tjrq,0,4)||'0101';

       select jyr into v_jsrq from dsc_cfg.t_xtjyr t where t.zrr =  i_tjrq;

       --select (to_date(v_jsrq,'yyyyMMdd')-to_date(v_ksrq,'yyyyMMdd')+1) into v_rqje  from dual ;

       select count(distinct(jyr)) into v_rqje from dsc_cfg.t_xtjyr where zrr between v_ksrq and v_jsrq;

    open cur_Result for
    select z.*, rownum-1 as PM from (
            select decode(n.orgcode,'','总计',n.orgcode) YYB,
             sum(n.PTH)                      PTH,
             sum(n.PTYXH )                   PTYXH,
             sum(n.GJZH )                    GJZH,
             sum(n.CGJZH)                    CGJZH,
             sum(n.xklrqx)                   XKLRQX,
             sum(n.gpqqqxkt)                 GPQQQXKT,
             --sum(m.lrrjye)                   XKLRQXRJYE,
             sum(m.xklrqxjf)                 XKLRQXRJYEJF,
             --sum(n.PTHJF)                    PTHJF,
             --sum(n.PTYXHJF)                  PTYXHJF,
             --sum(n.GJZHJF)                   GJZHJF,
             --sum(n.CGJZHJF)                  CGJZHJF,
             --sum(n.GPQQQXKTJF)               GPQQQXKTJF,
             --nvl(sum(n.jf ),0)               JF,
             nvl(sum(n.jf ),0) + nvl(sum(m.xklrqxjf),0)                     ZJF
      from(
      select t3.orgcode,
             sum(case when t2.khh is null then 1 end) PTH,
             sum(case when t2.yxhlx = 0 then 1 end)     PTYXH,
             sum(case when t2.yxhlx = 1 then 1 end)     GJZH,
             nvl(sum(case when t2.yxhlx = 2 then 1 end),0)     CGJZH,
             nvl(t6.xklrqx,0)       xklrqx,
             nvl(t5.gpqqqxkt,0)     gpqqqxkt,
             --nvl(t7.xklrqxjf,0)       xklrqxjf,
             nvl(sum(case when t2.khh is null then 1 end),0) PTHJF,
             nvl(sum(case when t2.yxhlx = 0 then 1 end),0) * 50 PTYXHJF,
             nvl(sum(case when t2.yxhlx = 1 then 1 end),0) * 250 GJZHJF,
             nvl(sum(case when t2.yxhlx = 2 then 1 end),0) * 500 CGJZHJF,
             nvl(t5.gpqqqxkt,0)*20 GPQQQXKTJF,
             nvl(sum(case when t2.khh is null then 1 end),0) +
             nvl(sum(case when t2.yxhlx = 0 then 1 end),0) * 50 +
             nvl(sum(case when t2.yxhlx = 1 then 1 end),0) * 250 +
             nvl(sum(case when t2.yxhlx = 2 then 1 end),0) * 500 +
             nvl(t5.gpqqqxkt,0)*20
             jf

             from crmii.tkhxx t
             left join crmii.T_NJZQ_YYBYXKHMXB t2 on t.khh = t2.khh and t2.tjrq = v_jsrq
             left join crmii.lborganization t3 on t.yyb = t3.orgcode
             --left join lborganization t4 on t3.fid = t4.orgcode
             left join (select t.yyb,count(distinct(t.khh)) gpqqqxkt from dsc_stat.t_stat_khywqxktqk t
                         left join crmii.tkhxx t1 on t.khh = t1.khh
                         ,crmii.TJGDM J,crmii.TXTDM X
                         where t1.CGYH=J.IBM
                               AND J.JGLB=2
                               and t1.khzt=x.IBM
                               and x.fldm='KHZT'
                                   and t.ktywqx in (402,403)
                                   and t.khrq  between v_ksrq and v_jsrq
                                   and t1.khzt = 0
                                   --and t.yyb = 0280
                                   group by t.yyb)t5
                 on t3.orgcode = t5.yyb
             left join (select t.yyb,count(distinct(t.khh)) xklrqx  from dsc_stat.t_stat_khywqxktqk t
                         left join crmii.tkhxx t1 on t.khh = t1.khh
                         ,crmii.TJGDM J,crmii.TXTDM X
                         where t1.CGYH=J.IBM
                               AND J.JGLB=2
                               and t1.khzt=x.IBM
                               and x.fldm='KHZT'
                                   and t.ktywqx in (112)
                                   and t.khrq  between v_ksrq and v_jsrq
                                   and t1.khzt = 0
                                   --and t.yyb = 0280
                                   group by t.yyb) t6
                 on t3.orgcode = t6.yyb

             ,crmii.TJGDM J,crmii.TXTDM X
               where t.CGYH=J.IBM
                     AND J.JGLB=2
                     and t.khzt=x.IBM
                     and x.fldm='KHZT'
                     and t.khrq between v_ksrq and v_jsrq
                     and t.khzt = 0
             --and t3.orgcode = 3305
             group by t3.orgcode,t5.gpqqqxkt, t6.xklrqx
             )n --
             left join (select t2.yyb,
                           sum(case when t3.lrrjye < 100000 then 100
                                 when t3.lrrjye >= 100000 and t3.lrrjye < 500000 then 300
                                 when t3.lrrjye >= 500000 and t3.lrrjye < 1000000 then 400
                                 when t3.lrrjye >= 1000000 and t3.lrrjye < 5000000 then 700
                                 when t3.lrrjye >= 5000000 and t3.lrrjye < 10000000 then 1100
                                 when t3.lrrjye >= 10000000  then 3100
                           end) xklrqxjf,
                           sum(t3.lrrjye) lrrjye
                          from (select distinct (t.khh) khh
                                  from dsc_stat.t_stat_khywqxktqk t
                                 where t.khrq between v_ksrq and v_jsrq
                                   and t.ktywqx = 112) t1
                          left join crmii.tkhxx t2 on t1.khh = t2.khh
                          left join (select t.khh, sum(t.rzje + t.rqje) / v_rqje lrrjye
                                       from dsc_stat.t_stat_rzrq_r t
                                      where t.rq between v_ksrq and v_jsrq
                                      group by t.khh) t3 on t1.khh = t3.khh
                                      group by t2.yyb) m
           on m.yyb = n.orgcode
             group by rollup(n.orgcode)
             order by nvl(sum(n.jf ),0) + nvl(sum(m.xklrqxjf),0) desc
             ) z;








end PCX_NJZQ_BJQH;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_CXLRGJZDKH
  (
    o_cursor OUT sys_refcursor
    ,I_RQ         in number
    ,I_ZBKJZDMIN   in varchar2
    ,I_ZBKJZDMAX  in varchar2
    ,I_GGJZDMIN   in varchar2
    ,I_GGJZDMAX   in varchar2
    ,I_WCDBBLMIN  in varchar2
    ,I_WCDBBLMAX  in varchar2
    ,I_YYB        in varchar2



  )
is
/*
  功能说明:查询两融高集中度客户
1、查询条件：
2、查询结果
  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称： PCX_NJZQ_CXLRGJZDKH
  -------------------------------------------------------------------------------
  修改记录
  2020.12.07 刘永舜 过程新建

*/

verrmsg      varchar2(4000);
V_ZBKJZDMIN  number(16,4);
V_ZBKJZDMAX  number(16,4);
V_GGJZDMIN   number(16,4);
V_GGJZDMAX   number(16,4);
V_WCDBBLMIN  number(16,4);
V_WCDBBLMAX  number(16,4);
begin
  V_ZBKJZDMIN :=  to_number(I_ZBKJZDMIN/100);
  V_ZBKJZDMAX :=  to_number(I_ZBKJZDMAX/100);
  V_GGJZDMIN   :=  I_GGJZDMIN /100;
  V_GGJZDMAX   :=  I_GGJZDMAX /100;
  V_WCDBBLMIN :=  I_WCDBBLMIN/100;
  V_WCDBBLMAX  :=  I_WCDBBLMAX/100;
  open o_cursor for

        select
             t1.yyb,
             t1.khh,
             t4.khxm,
             t1.grjg,
             t1.zzc,
             t1.zfz,
             decode (nvl(t1.zzc,0),0,0,(nvl(t2.HZZ ,0))/(t1.zzc))*100 HZZJZD,
             decode (nvl(t1.zzc,0),0,0,(nvl(t2.ZCZ ,0))/(t1.zzc))*100 ZCZJZD,
             decode (nvl(t1.zzc,0),0,0,(nvl(t2.KCB ,0))/(t1.zzc))*100 KCBJZD,
             decode (nvl(t1.zzc,0),0,0,(nvl(t2.HZZ ,0)+ nvl(t2.ZCZ ,0)+nvl(t2.KCB ,0))/(t1.zzc))*100 ZBKJZD,
             decode(nvl(t1.zzc,0),0,0,nvl(t3.zxsz,0)/(t1.zzc))*100 GGJZD,
             t1.dbbl*100 dbbl,
             t4.sj,
             t4.email
             from
      dsc_stat.t_stat_rzrq_r t1
      left join
      (select t.khh,
             nvl(sum(case when t.zqlb = 'C0' then (t.zxsz) end ),0) HZZ,        --创业板
             nvl(sum(case when t.zqlb in('CR','RC') then (t.zxsz) end ),0) ZCZ,  --注册创业/注创CDR
             nvl(sum(case when t.zqlb = 'AK' then (t.zxsz) end ),0) KCB  --科创板
        from (select t1.khh, t1.zqlb, t1.zxsz from dsc_bas.t_xy_zqye t1 where t1.rq = I_RQ
             union all
             select t2.khh, t2.zqlb, t2.zxsz from dsc_his.t_xy_zqyels t2 where t2.rq = I_RQ) t
      -- where t.rq = I_RQ
       group by t.khh
      )t2 on t1.khh = t2.khh
      left join
      (
      select t.khh,
             t.zqdm,
             t.zxsz,
             row_number() over(partition by t.khh order by t.zxsz desc) rn
        from (select t1.khh, t1.zqdm, t1.zqlb, t1.zxsz from dsc_bas.t_xy_zqye t1 where t1.rq = I_RQ
             union all
             select t2.khh, t2.zqdm, t2.zqlb, t2.zxsz from dsc_his.t_xy_zqyels t2 where t2.rq = I_RQ) t
      -- where t.rq = I_RQ
       --and t.khh = '120098205066'
      )t3  on t1.khh = t3.khh and t3.rn = 1
      left join crmii.tkhxx t4 on t1.khh = t4.khh
         where t1.rq = I_RQ
         and (((I_ZBKJZDMIN is null and I_ZBKJZDMAX IS NULL) or (decode (nvl(t1.zzc,0),0,0,(nvl(t2.HZZ ,0)+ nvl(t2.ZCZ ,0)+nvl(t2.KCB ,0))/(t1.zzc))*100 between I_ZBKJZDMIN AND I_ZBKJZDMAX))
         or ((I_GGJZDMIN  IS NULL AND I_GGJZDMAX IS NULL ) or (decode (nvl(t1.zzc,0),0,0,nvl(t3.zxsz,0)/(t1.zzc))*100 between I_GGJZDMIN and I_GGJZDMAX)))
         and ((I_WCDBBLMIN IS NULL AND I_WCDBBLMAX IS NULL) or (t1.dbbl*100 BETWEEN I_WCDBBLMIN AND I_WCDBBLMAX))
         and  (t4.yyb = I_YYB or I_YYB = 1) ;


 exception
   when others then
    verrmsg:=sqlerrm;
      open o_cursor for select'查询两融高集中度客户-'||verrmsg 错误  from dual;


end PCX_NJZQ_CXLRGJZDKH;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_G16XKHCWJJBB(
  cur_Result OUT sys_refcursor,
  i_khksrq       in  number,--开户开始日期
  i_khjsrq       in  number,--开户结束日期
  i_jyksrq       in  number,--交易开始日期
  i_jyjsrq       in  number,--交易结束日期
  i_khlx         in  number --客户类型0-新客户，1-老客户
  )
is

/*
  功能说明:G16-新开户场外基金报表

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);
V_KHLX  NUMBER;
v_sql varchar2(4000);
begin


V_KHLX := i_khlx;
v_sql := 'select 1 from dual';

    if V_KHLX = '0' then
    begin
    open cur_Result for
      select z.qrrq JYYF,
             sum(z.N1+z.N2+z.N3+z.N4+z.N5+z.N6) N7,
             sum(z.N1) N1,
             sum(z.N2) N2,
             sum(z.N3) N3,
             sum(z.N4) N4,
             sum(z.N5) N5,
             sum(z.N6) N6
             from (
               select t1.khh,t2.csrq,substr(t1.qrrq,0,6) qrrq,
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <=20 then sum(t1.qrje) else 0 end) N1,--20以下（含20）
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 20
                      and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 30 then sum(t1.qrje) else 0 end) N2,--20＜年龄≤30
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 30
                      and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 40 then sum(t1.qrje) else 0 end) N3,--30＜年龄≤40
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 40
                      and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 50 then sum(t1.qrje) else 0 end) N4,--40＜年龄≤50
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 50
                      and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 60 then sum(t1.qrje) else 0 end) N5,--50＜年龄≤60
                (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 60 then sum(t1.qrje) else 0 end) N6 --60以上
                 from
                 dsc_his.t_fp_jgmxls_his t1
                 left join crmii.tkhxx t2 on t1.khh = t2.khh
                 left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                 where t1.qrrq between i_jyksrq and i_jyjsrq
                 and t2.khrq between i_khksrq and i_khjsrq
                 and t2.csrq is not null
                 and t2.csrq <> 0
                 AND t1.CPDM != 'A72001'
                 and t1.APP_ID = 2
                 and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                 and t3.cplb in (1,2,3,4,18)
                 group by t2.csrq,t1.khh,t1.qrrq

                 union all

                select t1.khh,t2.csrq,substr(t1.qrrq,0,6) qrrq,
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <=20 then sum(t1.qrje) else 0 end) N1,--20以下（含20）
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 20
                        and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 30 then sum(t1.qrje) else 0 end) N2,--20＜年龄≤30
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 30
                        and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 40 then sum(t1.qrje) else 0 end) N3,--30＜年龄≤40
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 40
                        and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 50 then sum(t1.qrje) else 0 end) N4,--40＜年龄≤50
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 50
                        and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 60 then sum(t1.qrje) else 0 end) N5,--50＜年龄≤60
                  (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 60 then sum(t1.qrje) else 0 end) N6 --60以上
                   from
                   dsc_bas.t_fp_jgmxls t1
                   left join crmii.tkhxx t2 on t1.khh = t2.khh
                   left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                   where t1.qrrq between i_jyksrq and i_jyjsrq
                   and t2.khrq between i_khksrq and i_khjsrq
                   and t2.csrq is not null
                   and t2.csrq <> 0
                   AND t1.CPDM != 'A72001'
                   and t1.APP_ID = 2
                   and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                   and t3.cplb in (1,2,3,4,18)
                   group by t2.csrq,t1.khh,t1.qrrq
             )z
             group by z.qrrq
             order by z.qrrq desc;
             --select 1 from dual;
             --close cur_Result;
          end;
        elsif V_KHLX = '1' then
          open cur_Result for
          select z.qrrq JYYF,
                 sum(z.N1+z.N2+z.N3+z.N4+z.N5+z.N6) N7,
                 sum(z.N1) N1,
                 sum(z.N2) N2,
                 sum(z.N3) N3,
                 sum(z.N4) N4,
                 sum(z.N5) N5,
                 sum(z.N6) N6
                 from (
                   select t1.khh,t2.csrq,substr(t1.qrrq,0,6) qrrq,
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <=20 then sum(t1.qrje) else 0 end) N1,--20以下（含20）
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 20
                          and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 30 then sum(t1.qrje) else 0 end) N2,--20＜年龄≤30
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 30
                          and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 40 then sum(t1.qrje) else 0 end) N3,--30＜年龄≤40
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 40
                          and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 50 then sum(t1.qrje) else 0 end) N4,--40＜年龄≤50
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 50
                          and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 60 then sum(t1.qrje) else 0 end) N5,--50＜年龄≤60
                    (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 60 then sum(t1.qrje) else 0 end) N6 --60以上
                     from
                     dsc_his.t_fp_jgmxls_his t1
                     left join crmii.tkhxx t2 on t1.khh = t2.khh
                     left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                     where t1.qrrq between i_jyksrq and i_jyjsrq
                     and t2.khrq <= i_khjsrq -- 老客户
                     and t2.csrq is not null
                     and t2.csrq <> 0
                     and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                     AND t1.CPDM != 'A72001'
                     and t1.APP_ID = 2
                     and t3.cplb in (1,2,3,4,18)
                     group by t2.csrq,t1.khh,t1.qrrq

                     union all

                    select t1.khh,t2.csrq,substr(t1.qrrq,0,6) qrrq,
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <=20 then sum(t1.qrje) else 0 end) N1,--20以下（含20）
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 20
                            and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 30 then sum(t1.qrje) else 0 end) N2,--20＜年龄≤30
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 30
                            and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 40 then sum(t1.qrje) else 0 end) N3,--30＜年龄≤40
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 40
                            and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 50 then sum(t1.qrje) else 0 end) N4,--40＜年龄≤50
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 50
                            and months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 <= 60 then sum(t1.qrje) else 0 end) N5,--50＜年龄≤60
                      (case when months_between(to_date(to_char(sysdate,'yyyyMMdd'),'yyyyMMdd'),to_date(t2.csrq,'yyyyMMdd'))/12 > 60 then sum(t1.qrje) else 0 end) N6 --60以上
                       from
                       dsc_bas.t_fp_jgmxls t1
                       left join crmii.tkhxx t2 on t1.khh = t2.khh
                       left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                       where t1.qrrq between i_jyksrq and i_jyjsrq
                       and t2.khrq <= i_khjsrq -- 老客户
                       and t2.csrq is not null
                       and t2.csrq <> 0
                       AND t1.CPDM != 'A72001'
                       and t1.APP_ID = 2
                       and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                       and t3.cplb in (1,2,3,4,18)
                       group by t2.csrq,t1.khh,t1.qrrq
                 )z
                 group by z.qrrq
                 order by z.qrrq desc;
/*                 begin
                           open cur_Result for
                           select 1 from dual;
                           --close cur_Result;
                 end;*/
           end if;

         --select i_khksrq,i_khjsrq,i_jyksrq,i_jyjsrq from dual;

 exception
   when others then
    verrmsg:=sqlerrm;
      open cur_Result for select 'G16-新开户场外基金报表-'||verrmsg 错误  from dual;


end PCX_NJZQ_G16XKHCWJJBB;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_G16XKHCWJJBBACP(
  cur_Result OUT sys_refcursor,
  i_khksrq       in  number,--开户开始日期
  i_khjsrq       in  number,--开户结束日期
  i_jyksrq       in  number,--交易开始日期
  i_jyjsrq       in  number,--交易结束日期
  i_khlx         in  number --客户类型0-新客户，1-老客户
  )
is

/*
  功能说明:G16-新开户场外基金报表

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);

begin


    if i_khlx = '0' then
    open cur_Result for
      select z.qrrq,
             sum(z.N1+z.N2+z.N3+z.N4+z.N5) N7,
             sum(z.N1) N1,
             sum(z.N2) N2,
             sum(z.N3) N3,
             sum(z.N4) N4,
             sum(z.N5) N5,
             sum(z.N6) N6
             from (
             select substr(t1.qrrq,0,6) qrrq,
                 t3.cplb,
                 (case when t3.cplb = 1 then sum(t1.qrje) else 0 end) N1,-- 股票型
                 (case when t3.cplb = 2 then sum(t1.qrje) else 0 end) N2,-- 债券型
                 (case when t3.cplb = 3 then sum(t1.qrje) else 0 end) N3,-- 货币型
                 (case when t3.cplb = 4 then sum(t1.qrje) else 0 end) N4,-- 混合型
                 (case when t3.cplb = 18 then sum(t1.qrje) else 0 end) N5,-- 养老FOF
                 (case when t3.cplb is null  then sum(t1.qrje) else 0 end) N6-- 其他
                 from
                 dsc_his.t_fp_jgmxls_his t1
                 left join crmii.tkhxx t2 on t1.khh = t2.khh
                 left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm and t3.cplb in (1,2,3,4,18)
                 where t1.qrrq between i_jyksrq and i_jyjsrq
                 and t2.khrq between i_khksrq and i_khjsrq
                 --and t2.csrq is not null
                 --and t2.csrq <> 0
                 and t1.ywdm in (122,130,139)
                 AND t1.CPDM != 'A72001'
                 --and t1.APP_ID = 2
                 group by t3.cplb,t1.qrrq
             union all
             select substr(t1.qrrq,0,6) qrrq,
                 t3.cplb,
                 (case when t3.cplb = 1 then sum(t1.qrje) else 0 end) N1,-- 股票型
                 (case when t3.cplb = 2 then sum(t1.qrje) else 0 end) N2,-- 债券型
                 (case when t3.cplb = 3 then sum(t1.qrje) else 0 end) N3,-- 货币型
                 (case when t3.cplb = 4 then sum(t1.qrje) else 0 end) N4,-- 混合型
                 (case when t3.cplb = 18 then sum(t1.qrje) else 0 end) N5,-- 养老FOF
                 (case when t3.cplb is null  then sum(t1.qrje) else 0 end) N6-- 其他
                 from
                 dsc_bas.t_fp_jgmxls t1
                 left join crmii.tkhxx t2 on t1.khh = t2.khh
                 left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm and t3.cplb in (1,2,3,4,18)
                 where t1.qrrq between i_jyksrq and i_jyjsrq
                 and t2.khrq between i_khksrq and i_khjsrq
                 --and t2.csrq is not null
                 --and t2.csrq <> 0
                 and t1.ywdm in (122,130,139)
                 AND t1.CPDM != 'A72001'
                 --and t1.APP_ID = 2
                 group by t3.cplb,t1.qrrq
             )z
             group by z.qrrq
             order by z.qrrq desc;
         --select i_khksrq,i_khjsrq,i_jyksrq,i_jyjsrq from dual;
          elsif i_khlx = '1' then
            open cur_Result for
            select z.qrrq,
             sum(z.N1+z.N2+z.N3+z.N4+z.N5) N7,
             sum(z.N1) N1,
             sum(z.N2) N2,
             sum(z.N3) N3,
             sum(z.N4) N4,
             sum(z.N5) N5,
             sum(z.N6) N6
                   from (
                   select substr(t1.qrrq,0,6) qrrq,
                       t3.cplb,
                       (case when t3.cplb = 1 then sum(t1.qrje) else 0 end) N1,-- 股票型
                       (case when t3.cplb = 2 then sum(t1.qrje) else 0 end) N2,-- 债券型
                       (case when t3.cplb = 3 then sum(t1.qrje) else 0 end) N3,-- 货币型
                       (case when t3.cplb = 4 then sum(t1.qrje) else 0 end) N4,-- 混合型
                       (case when t3.cplb = 18 then sum(t1.qrje) else 0 end) N5,-- 养老FOF
                       (case when t3.cplb is null  then sum(t1.qrje) else 0 end) N6-- 其他
                       from
                       dsc_his.t_fp_jgmxls_his t1
                       left join crmii.tkhxx t2 on t1.khh = t2.khh
                       left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm and t3.cplb in (1,2,3,4,18)
                       where t1.qrrq between i_jyksrq and i_jyjsrq
                       and t2.khrq <= i_khjsrq --老客户
                       --and t2.csrq is not null
                       --and t2.csrq <> 0
                       AND t1.CPDM != 'A72001'
                       --and t1.APP_ID = 2
                       and t1.ywdm in (122,130,139)
                       group by t3.cplb,t1.qrrq
                   union all
                   select substr(t1.qrrq,0,6) qrrq,
                       t3.cplb,
                       (case when t3.cplb = 1 then sum(t1.qrje) else 0 end) N1,-- 股票型
                       (case when t3.cplb = 2 then sum(t1.qrje) else 0 end) N2,-- 债券型
                       (case when t3.cplb = 3 then sum(t1.qrje) else 0 end) N3,-- 货币型
                       (case when t3.cplb = 4 then sum(t1.qrje) else 0 end) N4,-- 混合型
                       (case when t3.cplb = 18 then sum(t1.qrje) else 0 end) N5,-- 养老FOF
                       (case when t3.cplb is null  then sum(t1.qrje) else 0 end) N6-- 其他
                       from
                       dsc_bas.t_fp_jgmxls t1
                       left join crmii.tkhxx t2 on t1.khh = t2.khh
                       left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm and t3.cplb in (1,2,3,4,18)
                       where t1.qrrq between i_jyksrq and i_jyjsrq
                       and t2.khrq <= i_khjsrq --老客户
                       --and t2.csrq is not null
                       --and t2.csrq <> 0
                       AND t1.CPDM != 'A72001'
                       --and t1.APP_ID = 2
                       and t1.ywdm in (122,130,139)
                       group by t3.cplb,t1.qrrq
                   )z
                   group by z.qrrq
                   order by z.qrrq desc;
            end if;
 exception
   when others then
    verrmsg:=sqlerrm;
      open cur_Result for select 'G16-新开户场外基金报表-'||verrmsg 错误  from dual;


end PCX_NJZQ_G16XKHCWJJBBACP;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_G16XKHCWJJJYRSACP(
  cur_Result OUT sys_refcursor,
  i_khksrq       in  number,--开户开始日期
  i_khjsrq       in  number,--开户结束日期
  i_jyksrq       in  number,--交易开始日期
  i_jyjsrq       in  number,--交易结束日期
  i_khlx         in  number --客户类型0-新客户，1-老客户
  )
is

/*
  功能说明:G16-新开户场外基金报表

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);

begin


    if i_khlx = '0' then
    open cur_Result for
      select substr(i_jyjsrq,0,6) JYYF,
             sum(z.N1+z.N2+z.N3+z.N4+z.N5) N7,
             sum(z.N1) N1,
             sum(z.N2) N2,
             sum(z.N3) N3,
             sum(z.N4) N4,
             sum(z.N5) N5,
             sum(z.N6) N6
             from (
             select --substr(n.qrrq,0,6) qrrq,
             n.cplb,
             nvl(sum((case when n.cplb = 1 then 1 else 0 end)),0) N1,-- 股票型
             nvl(sum((case when n.cplb = 2 then 1 else 0 end)),0) N2,-- 债券型
             nvl(sum((case when n.cplb = 3 then 1 else 0 end)),0) N3,-- 货币型
             nvl(sum((case when n.cplb = 4 then 1 else 0 end)),0) N4,-- 混合型
             nvl(sum((case when n.cplb = 18 then 1 else 0 end)),0) N5,-- 养老FOF
             nvl(sum((case when n.cplb is null  then 1 else 0 end)),0) N6-- 其他
             from (
                select distinct (t1.khh),t3.cplb
                   from
                   dsc_his.t_fp_jgmxls_his t1
                   left join crmii.tkhxx t2 on t1.khh = t2.khh
                   left join dsc_cfg.t_jrcpdm t3  on t1.cpdm = t3.cpdm
                   where t1.qrrq between i_jyksrq and i_jyjsrq
                   and t2.khrq between i_khksrq and i_khjsrq
                   --and t2.csrq is not null
                   --and t2.csrq <> 0
                   and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                   AND t1.CPDM != 'A72001'
                   and t3.cplb in (1,2,3,4,18)
                   --and t1.APP_ID = 2
                   --and t3.cplb is not null
                   group by t1.khh,t3.cplb
             ) n
             group by  n.cplb
             union all
             select --substr(n.qrrq,0,6) qrrq,
               n.cplb,
               nvl(sum((case when n.cplb = 1 then 1 else 0 end)),0) N1,-- 股票型
               nvl(sum((case when n.cplb = 2 then 1 else 0 end)),0) N2,-- 债券型
               nvl(sum((case when n.cplb = 3 then 1 else 0 end)),0) N3,-- 货币型
               nvl(sum((case when n.cplb = 4 then 1 else 0 end)),0) N4,-- 混合型
               nvl(sum((case when n.cplb = 18 then 1 else 0 end)),0) N5,-- 养老FOF
               nvl(sum((case when n.cplb is null  then 1 else 0 end)),0) N6-- 其他
               from (
                  select distinct (t1.khh),t3.cplb
                     from
                     dsc_bas.t_fp_jgmxls t1
                     left join crmii.tkhxx t2 on t1.khh = t2.khh
                     left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                     where t1.qrrq between i_jyksrq and i_jyjsrq
                     and t2.khrq between i_khksrq and i_khjsrq
                     --and t2.csrq is not null
                     --and t2.csrq <> 0
                     and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                     AND t1.CPDM != 'A72001'
                     and t3.cplb in (1,2,3,4,18)
                     --and t1.APP_ID = 2
                     --and t3.cplb is not null
                     group by t1.khh,t3.cplb
               ) n
               group by  n.cplb
             )z

            ;
        elsif i_khlx = '1' then
         open cur_Result for
          select substr(i_jyjsrq,0,6) JYYF,
             sum(z.N1+z.N2+z.N3+z.N4+z.N5) N7,
             sum(z.N1) N1,
             sum(z.N2) N2,
             sum(z.N3) N3,
             sum(z.N4) N4,
             sum(z.N5) N5,
             sum(z.N6) N6
                 from (
                 select --substr(n.qrrq,0,6) qrrq,
                 n.cplb,
                 sum((case when n.cplb = 1 then 1 else 0 end)) N1,-- 股票型
                 sum((case when n.cplb = 2 then 1 else 0 end)) N2,-- 债券型
                 sum((case when n.cplb = 3 then 1 else 0 end)) N3,-- 货币型
                 sum((case when n.cplb = 4 then 1 else 0 end)) N4,-- 混合型
                 sum((case when n.cplb = 18 then 1 else 0 end)) N5,-- 养老FOF
                 sum((case when n.cplb is null  then 1 else 0 end)) N6-- 其他
                 from (
                    select distinct (t1.khh),t3.cplb
                       from
                       dsc_his.t_fp_jgmxls_his t1
                       left join crmii.tkhxx t2 on t1.khh = t2.khh
                       left join dsc_cfg.t_jrcpdm t3 on  t1.cpdm = t3.cpdm
                       where t1.qrrq between i_jyksrq and i_jyjsrq
                       and t2.khrq <= i_khjsrq
                       --and t2.csrq is not null
                       --and t2.csrq <> 0
                       and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                       AND t1.CPDM != 'A72001'
                       and t3.cplb in (1,2,3,4,18)
                       --and t1.APP_ID = 2
                       --and t3.cplb is not null
                       group by t1.khh,t3.cplb
                 ) n
                 group by  n.cplb
                 union all
                 select --substr(n.qrrq,0,6) qrrq,
                   n.cplb,
                   sum((case when n.cplb = 1 then 1 else 0 end)) N1,-- 股票型
                   sum((case when n.cplb = 2 then 1 else 0 end)) N2,-- 债券型
                   sum((case when n.cplb = 3 then 1 else 0 end)) N3,-- 货币型
                   sum((case when n.cplb = 4 then 1 else 0 end)) N4,-- 混合型
                   sum((case when n.cplb = 18 then 1 else 0 end)) N5,-- 养老FOF
                   sum((case when n.cplb is null  then 1 else 0 end)) N6-- 其他
                   from (
                      select distinct (t1.khh),t3.cplb
                         from
                         dsc_bas.t_fp_jgmxls t1
                         left join crmii.tkhxx t2 on t1.khh = t2.khh
                         left join dsc_cfg.t_jrcpdm t3 on t1.cpdm = t3.cpdm
                         where t1.qrrq between i_jyksrq and i_jyjsrq
                         and t2.khrq <= i_khjsrq
                         --and t2.csrq is not null
                         --and t2.csrq <> 0
                         and t1.ywdm in (122,130,139)   -- 122申购；130认购结果；139定时定额投资
                         AND t1.CPDM != 'A72001'
                         and t3.cplb in (1,2,3,4,18)
                         --and t1.APP_ID = 2
                         --and t3.cplb is not null
                         group by t1.khh,t3.cplb
                   ) n
                   group by  n.cplb
                 )z;
        end if;
         --select i_khksrq,i_khjsrq,i_jyksrq,i_jyjsrq from dual;

 exception
   when others then
    verrmsg:=sqlerrm;
      open cur_Result for select 'G16-新开户场外基金报表-'||verrmsg 错误  from dual;


end PCX_NJZQ_G16XKHCWJJJYRSACP;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_JLPTG(cur_Result OUT sys_refcursor) is

  /*
    功能说明:百家争先营业部排名-金罗盘推广

    入口参数：

    输出内容：

    设计说明
        数据来源：
        过程名称：

    -------------------------------------------------------------------------------
    修改记录
    2020.08.13 刘永舜 过程新建

  */

  verrmsg varchar2(4000);

begin

  open cur_Result for
    SELECT decode(t.yyb,'','总计',t.yyb) YYB,
           sum(t.wcsl) wcsl,
           sum(t.mbsl) mbsl,
           to_char(round((sum(t.wcsl) / sum(t.mbsl)*100), 4), 'FM90.90') || '%' wcl
      FROM crmii.T_NJZQ_JLPMB t
     group by rollup(t.yyb)
     order by sum(t.wcsl) desc;

end PCX_NJZQ_JLPTG;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_KHLRRJYE(cur_Result OUT sys_refcursor,
                                              i_khh      in varchar2, --客户号
                                              i_khksrq   in number, --开户开始日期
                                              i_khjsrq   in number, --开户结束日期
                                              i_jyksrq   in number, --交易开始日期
                                              i_jyjsrq   in number, --交易结束日期
                                              i_yyb      in varchar2

                                              ) is

  /*
    功能说明:客户两融日均余额

    入口参数：

    输出内容：

    设计说明
        数据来源：
        过程名称：

    -------------------------------------------------------------------------------
    修改记录
    2020.10.21 刘永舜 过程新建

  */

  verrmsg varchar2(4000);
  v_rqje  number(16);
  v_yyb   number(4);

begin
  select count(distinct(jyr)) into v_rqje from dsc_cfg.t_xtjyr where zrr between i_jyksrq and i_jyjsrq;
  v_yyb := lpad(i_yyb,4,0);
  --select (to_date(i_jyjsrq,'yyyyMMdd')-to_date(i_jyksrq,'yyyyMMdd')+1) into v_rqje  from dual ;

  open cur_Result for
  --select 1 from dual;

select
       t1.yyb                YYBMC,
       t1.yyb                YYBDM,
       t2.khxm               KHXM,
       t1.khh                KHH,
       sum(t5.rzje)/ v_rqje  RZRJYE,
       sum(t5.rqje)/ v_rqje  RQRJYE,
       (sum(t5.rzje)+sum(t5.rqje))/ v_rqje LRRJYE,
       t1.khrq               LRKHRQ,
       t2.khrq               PTKHRQ--,
       --i_yyb                 YYB
       --t4.khrq               LRQXKTRQ

  from dsc_bas.t_khxx_rzrq t1
  left join crmii.tkhxx t2 on t1.khh = t2.khh
  left join crmii.lborganization t3 on t1.yyb = t3.orgcode
  left join crmii.lborganization t4 on t3.fid = t4.orgcode
  left join dsc_stat.t_stat_rzrq_r t5 on t1.khh = t5.khh
     where ((i_jyksrq is null and i_jyjsrq is null) or (t5.rq between i_jyksrq and i_jyjsrq))
       and ((i_khksrq is null and i_khjsrq is null) or (t1.khrq between i_khksrq and i_khjsrq))
       and ((i_khh is null ) or (t1.khh = i_khh))
       and ( ('1' = v_yyb) or (v_yyb is null )  or (t3.orgcode = v_yyb) or (t4.orgcode = v_yyb))
    --and t.khh = '000001000101'
     group by t1.yyb, t2.khxm, t1.khh, t1.khrq, t2.khrq;

exception
  when others then
    verrmsg := sqlerrm;
    open cur_Result for
      select '客户两融日均余额-' || verrmsg 错误 from dual;

end PCX_NJZQ_KHLRRJYE;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_LRYE(
  cur_Result OUT sys_refcursor,
  i_tjyf       in  number       --统计月份
  )
is

/*
  功能说明:百家争先营业部排名-百家千户

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);
v_qnksrq  number(8);
v_qnjsrq  number(8);
v_jnksrq  number(8);
v_jnjsrq  number(8);
v_qnrqjg  number(8);
v_jnrqjg  number(8);

begin

v_qnksrq := (substr(i_tjyf,0,4)-1)||'0101';
v_qnjsrq := (substr(i_tjyf,0,4)-1)||1231;
v_jnksrq := (substr(i_tjyf,0,4))||'0101';

select max(jyr) into v_jnjsrq from dsc_cfg.t_xtjyr t where t.ny =  i_tjyf;

select to_date(v_qnjsrq,'yyyyMMdd')-to_date(v_qnksrq,'yyyyMMdd')+1 into v_qnrqjg from dual;
select to_date(v_jnjsrq,'yyyyMMdd')-to_date(v_jnksrq,'yyyyMMdd')+1 into v_jnrqjg from dual;



    open cur_Result for
/*      select decode(a.name,'','总计',a.name) YYB,
             sum(a.qnrjye) qnrjye,
             sum(b.jnrjye) jnrjye,
             sum(b.jnrjye - a.qnrjye)  jzz,
             to_char(round(decode (sum(a.qnrjye), 0 , -99, (sum( b.jnrjye-  a.qnrjye)/sum(a.qnrjye))),4),'FM990.90')||'%' zzl
             from (
             select t1.name,round((sum(t.rzje)+sum(t.rqje))/v_qnrqjg/10000,2) qnrjye
               from dsc_stat.t_stat_yyb_rzrq_r t
               left join crmii.lborganization t1 on t.yyb = t1.orgcode
               left join crmii.lborganization t2 on t1.fid = t2.orgcode
                where t.rq between v_qnksrq and v_qnjsrq
                --and t2.name = '南京分公司'
                group by t1.name) a,
           ( select t1.name,round((sum(t.rzje)+sum(t.rqje))/v_jnrqjg /10000,2) jnrjye
                 from dsc_stat.t_stat_yyb_rzrq_r t
                 left join crmii.lborganization t1 on t.yyb = t1.orgcode
                 left join crmii.lborganization t2 on t1.fid = t2.orgcode
                  where t.rq between v_jnksrq and v_jnjsrq
                  --and t2.name = '南京分公司'
                  group by t1.name)b
                  where a.name = b.name
                  group by rollup(a.name)
                  order by sum(b.jnrjye - a.qnrjye) desc;*/
        select decode(p.YYB,'','总计',q.name) YYB,
               p.qnrjye,
               p.jnrjye,
               p.jzz,
               to_char(p.zzl*100,'FM99990.90')||'%' zzl,
               rownum PM from (
        select o.yyb,
               sum(o.qnrjye) qnrjye,
               sum(o.jnrjye) jnrjye,
               sum(o.jzz)    jzz,
               sum(o.zzl)    zzl,
               sum(o.zzlpm)  zzlpm,
               sum(o.jzzpm)  jzzpm

                from (

        select z.yyb, z.qnrjye, z.jnrjye, z.jzz, z.zzl,z.zzlpm,y.jzzpm from (
        select n.*, rownum ZZLPM from (
        select a.yyb, b.LRRJYE qnrjye, a.LRRJYE jnrjye, nvl(a.LRRJYE,0)-nvl(b.LRRJYE,0) jzz, decode(nvl(b.LRRJYE,0), 0, nvl(a.LRRJYE,0)/600, (nvl(a.LRRJYE,0)-nvl(b.LRRJYE,0))/nvl(b.LRRJYE,0)) zzl
          from (select t1.yyb YYB,
                       --sum(t1.rzje)/ 245/10000  RZRJYE,
                       --sum(t1.rqje)/ 245/10000  RQRJYE,
                       (sum(t1.rzje) + sum(t1.rqje)) / v_jnrqjg / 10000 LRRJYE
                --i_yyb                 YYB
                --t4.khrq               LRQXKTRQ

                  from dsc_stat.t_stat_rzrq_r t1
                 where t1.rq between v_jnksrq and v_jnjsrq
                --and t1.yyb = 3126
                 group by t1.yyb) a
          left join

         (select t1.yyb YYB,
                 --sum(t1.rzje)/ 245/10000  RZRJYE,
                 --sum(t1.rqje)/ 245/10000  RQRJYE,
                 (sum(t1.rzje) + sum(t1.rqje)) / v_qnrqjg / 10000 LRRJYE
            from dsc_stat.t_stat_rzrq_r t1
           where t1.rq between v_qnksrq and v_qnjsrq
          --and t1.yyb = 3210
           group by t1.yyb) b on a.yyb = b.yyb
           order by decode(nvl(b.LRRJYE,0), 0, nvl(a.LRRJYE,0)/600, nvl(a.LRRJYE,0)/nvl(b.LRRJYE,0)) desc
           ) n
           ) z
           left join
           (
           select m.*, rownum JZZPM from (
        select a.yyb, nvl(a.LRRJYE,0)-nvl(b.LRRJYE,0) jzz
          from (select t1.yyb YYB,
                       --sum(t1.rzje)/ 245/10000  RZRJYE,
                       --sum(t1.rqje)/ 245/10000  RQRJYE,
                       (sum(t1.rzje) + sum(t1.rqje)) / v_jnrqjg / 10000 LRRJYE
                --i_yyb                 YYB
                --t4.khrq               LRQXKTRQ

                  from dsc_stat.t_stat_rzrq_r t1
                 where t1.rq between v_jnksrq and v_jnjsrq
                --and t1.yyb = 3126
                 group by t1.yyb) a
          left join

         (select t1.yyb YYB,
                 --sum(t1.rzje)/ 245/10000  RZRJYE,
                 --sum(t1.rqje)/ 245/10000  RQRJYE,
                 (sum(t1.rzje) + sum(t1.rqje)) / v_qnrqjg / 10000 LRRJYE
            from dsc_stat.t_stat_rzrq_r t1
           where t1.rq between v_qnksrq and v_qnjsrq
          --and t1.yyb = 3210
           group by t1.yyb) b on a.yyb = b.yyb
           order by nvl(a.LRRJYE,0)-nvl(b.LRRJYE,0) desc
           )m
           )y
           on z.yyb = y.yyb

           ) o
           group by rollup (o.yyb)
           order by (sum(o.zzlpm*0.5+o.jzzpm*0.5)) asc
           )p
           left join crmii.lborganization q
             on   p.yyb = q.orgcode;










end PCX_NJZQ_LRYE;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_RJBYL(cur_Result OUT sys_refcursor,
  i_ksyf       in  number,       --开始月份
  i_jsyf       in  number
  ) is

  /*
    功能说明:百家争先营业部排名-日均保有量

    入口参数：

    输出内容：

    设计说明
        数据来源：
        过程名称：

    -------------------------------------------------------------------------------
    修改记录
    2020.08.13 刘永舜 过程新建

  */

  verrmsg varchar2(4000);
  v_rqje  number(16);
  v_ksrq  number(8);
  v_jsrq  number(8);
begin

  select min(zrr) into v_ksrq from dsc_cfg.t_xtjyr where ny = i_ksyf;
  select max(zrr) into v_jsrq from dsc_cfg.t_xtjyr where ny = i_jsyf;

  select count(zrr) into v_rqje from dsc_cfg.t_xtjyr t where t.zrr between v_ksrq and v_jsrq;


  open cur_Result for
      select rownum PM , o.* from (
      select
             z.yyb,
             sum(z.rjbyl) rjbyl,
             sum(z.mbrw) mbrw,
             to_char(decode(z.yyb, '',sum(z.rjbyl)/sum(z.mbrw)*100, sum(z.wcl)*100),'FM99990.90')||'%' wcl

      from
      (
      select m.*, rownum WCLPM from (
      select t1.yyb,
             sum(t1.zsz) / v_rqje / 10000 rjbyl,
             t2.mbrw,
             sum(t1.zsz) / v_rqje / 10000 / t2.mbrw wcl
        from jjxsxt.t_rjbylrljgyyb t1
        left join crmii.T_NJZQ_JJRJBYLMB t2 on t1.yyb = t2.yyb
       where t1.yf between i_ksyf and i_jsyf
        --and t1.yyb = 0010
       group by t1.yyb, t2.mbrw
       order by sum(t1.zsz) / v_rqje / 10000 / t2.mbrw desc
       ) m
       ) z

       left join
       (
       select n.*, rownum BYLPM from
       (
      select t1.yyb,
             sum(t1.zsz) / v_rqje / 10000 rjbyl
        from jjxsxt.t_rjbylrljgyyb t1
       where t1.yf between i_ksyf and i_jsyf
        --and t1.yyb = 0010
       group by t1.yyb
       order by sum(t1.zsz) / v_rqje / 10000 desc
       )n
       )y
       on z.yyb = y.yyb
       group by rollup(z.yyb)
       order by sum(z.WCLPM*0.5+y.BYLPM*0.5) asc
       )o
       ;


end PCX_NJZQ_RJBYL;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_RXYY(
  cur_Result OUT sys_refcursor,
  i_tjrq       in  number       --统计月份
  )
is

/*
  功能说明:百家争先营业部排名-日新月益

  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称：

  -------------------------------------------------------------------------------
  修改记录
  2020.08.13 刘永舜 过程新建

*/

verrmsg varchar2(4000);
v_qnjsrq  number(8);

v_jnjsrq  number(8);


begin



select max(jyr) into v_qnjsrq from dsc_cfg.t_xtjyr t where t.ny =  (substr(i_tjrq,0,4)-1)||'12';
select jyr into v_jnjsrq from dsc_cfg.t_xtjyr t where t.zrr =  i_tjrq;

    open cur_Result for
select n.*, rownum as zhpm from (
 select t1.* from (
 select  z.*,rownum as rown from  (
      select
             a.yybbm   YYB,
             a.yybmc   yybmc,
             e.orgcode FGS,
             nvl(a.qnptyxh,0)   QNPTYXH,           -- 去年年末普通有效户
             nvl(a.qngjzyxh,0)  QNGJZYXH,          -- 去年年末高净值有效户
             nvl(a.qncgjzyxh,0) QNCGJZYXH,         -- 去年年末超高净值有效户
             nvl(b.jnptyxh,0)   JNPTYXH,           -- 今年普通有效户
             nvl(b.jngjzyxh,0)  JNGJZYXH,          -- 今年高净值有效户
             nvl(b.jncgjzyxh,0) JNCGJZYXH,         -- 今年超高净值有效户
             nvl(a.qnyxhzs,0)   QNYXHZS,           -- 去年年末有效户总数
             nvl(b.jnyxhzs,0)   JNYXHZS,           -- 今年有效户总数
             (nvl(b.jnyxhzs,0) - nvl(a.qnyxhzs,0)) JZZS,            --净增长数
             (nvl(b.jnptyxh,0)-nvl(a.qnptyxh,0))*50+(nvl(b.jngjzyxh,0)-nvl(a.qngjzyxh,0))*250+(nvl(b.jncgjzyxh,0)-nvl(a.qncgjzyxh,0))*500 JZZJF,  --净增长积分
             c.yxhndmbrw  NDMBRW,      -- 有效户年度目标任务
             to_char((nvl(b.jnyxhzs,0) - nvl(a.qnyxhzs,0))/c.yxhndmbrw*100,'FM990.90')||'%' WCL   -- 完成率

        from (select t.yybbm,
                     t.yybmc,
                     sum(case when t.yxhlx = 0 then 1 end) qnptyxh,
                     sum(case when t.yxhlx = 1 then 1 end) qngjzyxh,
                     sum(case when t.yxhlx = 2 then 1 end) qncgjzyxh,
                     count(*) qnyxhzs
                from crmii.T_NJZQ_YYBYXKHMXB t
               where t.tjrq = v_qnjsrq
                 --and t.yybbm = 3105
               group by t.yybbm, t.yybmc) a
        left join (

                   select t.yybbm,
                           t.yybmc,
                           sum(case when t.yxhlx = 0 then 1 end) jnptyxh,
                           sum(case when t.yxhlx = 1 then 1 end) jngjzyxh,
                           sum(case when t.yxhlx = 2 then 1 end) jncgjzyxh,
                           count(*) jnyxhzs
                     from crmii.T_NJZQ_YYBYXKHMXB t
                    where t.tjrq = v_jnjsrq
                      --and t.yybbm = 3105
                    group by t.yybbm, t.yybmc) b on a.yybbm = b.yybbm
      left join crmii.T_NJZQ_YXHMBRW c on lpad(a.yybbm,4,0) = c.yyb
      left join crmii.lborganization d on lpad(a.yybbm,4,0) = d.orgcode
      left join crmii.lborganization e on d.fid = e.orgcode
      order by (nvl(b.jnptyxh,0)-nvl(a.qnptyxh,0))*50+(nvl(b.jngjzyxh,0)-nvl(a.qngjzyxh,0))*250+(nvl(b.jncgjzyxh,0)-nvl(a.qncgjzyxh,0))*500 desc
      )z)t1
left join (
 select  y.*,rownum as rown from  (
      select
             a.yybbm   YYB,
             e.orgcode FGS,
             nvl(a.qnptyxh,0)   QNPTYXH,           -- 去年年末普通有效户
             nvl(a.qngjzyxh,0)  QNGJZYXH,          -- 去年年末高净值有效户
             nvl(a.qncgjzyxh,0) QNCGJZYXH,         -- 去年年末超高净值有效户
             nvl(b.jnptyxh,0)   JNPTYXH,           -- 今年普通有效户
             nvl(b.jngjzyxh,0)  JNGJZYXH,          -- 今年高净值有效户
             nvl(b.jncgjzyxh,0) JNCGJZYXH,         -- 今年超高净值有效户
             nvl(a.qnyxhzs,0)   QNYXHZS,           -- 去年年末有效户总数
             nvl(b.jnyxhzs,0)   JNYXHZS,           -- 今年有效户总数
             (nvl(b.jnyxhzs,0) - nvl(a.qnyxhzs,0)) JZZS,            --净增长数
             (nvl(b.jnptyxh,0)-nvl(a.qnptyxh,0))*50+(nvl(b.jngjzyxh,0)-nvl(a.qngjzyxh,0))*250+(nvl(b.jncgjzyxh,0)-nvl(a.qncgjzyxh,0))*500 JZZJF,  --净增长积分
             c.yxhndmbrw  NDMBRW,      -- 有效户年度目标任务
             to_char((nvl(b.jnyxhzs,0) - nvl(a.qnyxhzs,0))/c.yxhndmbrw*100,'FM990.90')||'%' WCL   -- 完成率

        from (select t.yybbm,
                     t.yybmc,
                     sum(case when t.yxhlx = 0 then 1 end) qnptyxh,
                     sum(case when t.yxhlx = 1 then 1 end) qngjzyxh,
                     sum(case when t.yxhlx = 2 then 1 end) qncgjzyxh,
                     count(*) qnyxhzs
                from crmii.T_NJZQ_YYBYXKHMXB t
               where t.tjrq = v_qnjsrq
                 --and t.yybbm = 3105
               group by t.yybbm, t.yybmc) a
        left join (

                   select t.yybbm,
                           t.yybmc,
                           sum(case when t.yxhlx = 0 then 1 end) jnptyxh,
                           sum(case when t.yxhlx = 1 then 1 end) jngjzyxh,
                           sum(case when t.yxhlx = 2 then 1 end) jncgjzyxh,
                           count(*) jnyxhzs
                     from crmii.T_NJZQ_YYBYXKHMXB t
                    where t.tjrq = v_jnjsrq
                      --and t.yybbm = 3105
                    group by t.yybbm, t.yybmc) b on a.yybbm = b.yybbm
      left join crmii.T_NJZQ_YXHMBRW c on lpad(a.yybbm,4,0) = c.yyb
      left join crmii.lborganization d on lpad(a.yybbm,4,0) = d.orgcode
      left join crmii.lborganization e on d.fid = e.orgcode
      order by (nvl(b.jnyxhzs,0) - nvl(a.qnyxhzs,0))/c.yxhndmbrw desc
      )y    )t2
      on t1.YYB = t2.YYB
      order by (t1.rown*0.5 + t2.rown*0.5) asc,t1.jzzjf desc
      ) n;









end PCX_NJZQ_RXYY;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_XKJGKHTB
  (
  o_ret_code   out  number,
  o_ret_note   out  varchar2
  )
is
/*
  功能说明:新开机构客户同步
1、查询条件：
2、查询结果
  入口参数：

  输出内容：

  设计说明
      数据来源：
      过程名称： PCX_NJZQ_XKJGKHTB
  -------------------------------------------------------------------------------
  修改记录
  20200709 刘永舜 过程新建

*/

verrmsg    varchar2(4000);
begin
    o_ret_code := 1;
    o_ret_note := '执行成功!';
    delete from crmii.T_NJZQ_JGYWKHSJGLB where khh in (select khh from crmii.T_NJZQ_XKJGKHB);
   insert into crmii.T_NJZQ_JGYWKHSJGLB (ID, KHH, YJBS, EJBS, SJBS)
   select SEQ_NJZQ_JGYWKHSJGL.NEXTVAL,
          t1.khh,
          t1.yjbs,
          t1.ejbs,
          t1.sjbs
   from crmii.T_NJZQ_XKJGKHB t1
   ;

   commit;




end PCX_NJZQ_XKJGKHTB;

CREATE OR REPLACE procedure NJZQ_XTGL.PCX_NJZQ_XSQKTJBB(
  cur_Result OUT sys_refcursor,
  i_ksrq       in  number,
  i_jsrq       in  number
  )
is

/*
  功能说明:网络金融部报表-销售情况统计报表
1、查询条件：营业部、开始日期、结束日期
2、查询结果 南京证券全部的以及金罗盘（zztzt）、鑫易通（TDX）和理财商城（tknsc）所销售的收益凭证、银行理财和场外开基的销售数据
  入口参数：

  输出内容：

  设计说明
      数据来源：dsc_his.t_fp_jrwtls_his;  DSC_HIS.T_FP_JGMXLS_HIS;  usr.tfp_ywsqls;  SRC_APEXFP.TFP_JGDM
      过程名称： PCX_NJZQ_XSQKTJBB

  -------------------------------------------------------------------------------
  修改记录
  2018.08.10 周显 过程新建
  2019.05.06 周显 修改       dsc_his.t_fp_jrwtls_his鑫易通的统计方式由原来的'TDX%'增加一个'CJ:TDX%'
  2019.06.03 周显 修改       dsc_his.t_fp_jrwtls_his的upper(CZZD)包括：'ZZTZT%'、 'TKNSC%'、'TDX%'、'NDZH%' 、'THS%'
                             usr.tfp_ywsqls的upper(CZZD)包括：'ZZTZT%'、 'TKNSC%'、'TDX%'
*/

verrmsg varchar2(4000);

begin

    execute immediate 'truncate table tempdata';

    --机构代码 C1， 产品代码 C2， 系统ID C3,  业务代码 C4， 操作站点 C6， 销售笔数 N1， 销售金额 N2， 销售人数 N3

    --OTC市场就是场外交易市场，YWDM 为('20','52')，OTC=收益凭证（证券协会）+银行理财，银行理财=南京银行+兴业银行
    --其中CLZT为-1 认购失败 删除此行数据
    --YWDM为52表示撤单 此行的YWTH为原认购单的WTH 同时删除撤单和原认购单所属行数据
    INSERT INTO NJZQ_XTGL.TEMPDATA(C1, C2, C4, C6, N2, N3)
           SELECT  A.WTJG, A.CPID, A.YWDM,
                   ( case when (upper(A.czzd) like 'ZZTZT%' or upper(A.czzd) like 'CJ:ZZTZT%') then '77775557' --金罗盘
                          when (upper(a.CZZD) like 'TDX%' or upper(a.CZZD) like 'CJ:TDX%' or upper(a.CZZD) like 'THS%' or upper(a.CZZD) like 'CJ:THS%' ) then '99991903' -- 鑫易通
                          when (upper(A.czzd) like 'NDZH%' or upper(A.czzd) like 'DZH%' or upper(A.czzd) like 'CJ:NDZH%') then '99990905'    -- 大智慧
                          when (upper(A.czzd) like 'CJ:TKNSC%' or upper(A.czzd) like 'TKNSC%') then '77775552'            --理财商城
                          end
                    )CZZD,
                   A.YWJE, A.KHH
                   FROM (  SELECT A.WTJG, A.CPID, A.YWDM, upper(substr(upper(A.czzd),0,40)) CZZD, A.YWJE, A.KHH
                                  FROM usr.tfp_ywsqls a
                           WHERE A.DJRQ BETWEEN i_ksrq AND i_jsrq
                                  AND A.YWDM IN('20')
                                  AND A.WTH NOT IN (SELECT A.YWTH FROM usr.tfp_ywsqls a WHERE A.DJRQ BETWEEN i_ksrq AND i_jsrq AND A.YWDM IN('52'))
                                  AND A.CLZT != '-1'
                         ) A;

    --基金数据，申购和认购时不使用dsc_his.t_fp_jrwtls_his中WTJE作为销售金额，而是使用dsc_his.T_FP_JGMXLS_HIS中的QRJE作为销售金额
    --赎回时使用dsc_his.T_FP_JGMXLS_HIS中的QRJE确认作为销售金额
    --两个表用SQBH匹配

    --申购和认购
    INSERT INTO NJZQ_XTGL.TEMPDATA(C1, C2, C3, C4, C6, N2, N3)
           SELECT  A.JRJGDM, A.CPDM, A.APP_ID, B.YWDM, C.wtgy CZZD, B.QRJE, A.KHH
                   FROM (  SELECT A.JRJGDM, A.CPDM, A.APP_ID, A.YWDM, upper(substr(A.CZZD,0,40)) CZZD, A.KHH, A.SQBH, A.WTJE
                                  FROM dsc_his.t_fp_jrwtls_his a
                           WHERE /*wtrq BETWEEN i_ksrq AND i_jsrq
                                 AND */A.cpdm != 'A72001'
                                 --AND A.YWDM IN(020,022,030,039)
                                 and APP_ID = 2
                         ) A,
                        (  SELECT B.QRJE, B.SQBH,B.khh, B.wtrq, B.yyb, B.wtfs,B.ywdm
                                  FROM DSC_HIS.T_FP_JGMXLS_HIS B
                                  left join dsc_cfg.t_jrcpdm t1 on b.cpdm = t1.cpdm
                           WHERE qrrq BETWEEN i_ksrq AND i_jsrq
                                 AND B.CPDM != 'A72001'
                                 and B.YWDM in (122,130,139)
                                 and B.APP_ID = 2
                                 and t1.cplb in (1,2,3,4,18)
                         ) B,
                         (select C.SQBH, C.khh, C.wtrq, C.yyb, C.wtfs, C.wtgy
                                 from usr.t_njzq_tof_jjwtls C
                         )C
                         where A.SQBH = B.SQBH
                         and B.khh = C.Khh
                         and B.sqbh = C.Sqbh
                         and B.wtrq = C.Wtrq
                         and B.yyb = C.yyb
                         and B.wtfs = C.wtfs;
    --赎回
    INSERT INTO NJZQ_XTGL.TEMPDATA(C1, C2, C3, C4, C6, N2, N3)
           SELECT  A.JRJGDM, A.CPDM, A.APP_ID, B.YWDM, C.wtgy CZZD, B.QRJE, A.KHH
                   FROM (  SELECT A.JRJGDM, A.CPDM, A.APP_ID, A.YWDM, upper(substr(A.CZZD,0,40)) CZZD, A.KHH, A.SQBH, A.WTJE
                                  FROM dsc_his.t_fp_jrwtls_his a
                           WHERE /*wtrq BETWEEN i_ksrq AND i_jsrq
                                 AND */A.cpdm != 'A72001'
                                 --AND A.YWDM IN (024,042)
                                 and APP_ID = 2
                         ) A,
                        (  SELECT B.QRJE, B.SQBH,B.khh, B.wtrq, B.yyb, B.wtfs,B.ywdm
                                  FROM DSC_HIS.T_FP_JGMXLS_HIS B
                                  left join dsc_cfg.t_jrcpdm t1 on b.cpdm = t1.cpdm
                           WHERE qrrq BETWEEN i_ksrq AND i_jsrq
                                 AND B.CPDM != 'A72001'
                                 and B.YWDM in(124,142)
                                 and B.APP_ID = 2
                                 and t1.cplb in (1,2,3,4,18)
                         ) B,
                         (select C.SQBH, C.khh, C.wtrq, C.yyb, C.wtfs, C.wtgy
                                 from usr.t_njzq_tof_jjwtls C
                         )C
                         where A.SQBH = B.SQBH
                         and B.khh = C.Khh
                         and B.sqbh = C.Sqbh
                         and B.wtrq = C.Wtrq
                         and B.yyb = C.yyb
                         and B.wtfs = C.wtfs;

     execute immediate 'truncate table tempdata_bb';

     --(C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, N1, N2, N3, N4, N5, N6, N7, N8, N9, N10, N11, N12)
     --分类，总笔数，总金额，总人数，金罗盘（笔数，金额，人数，笔数占比，金额占比，人数占比），鑫易通（笔数，金额，人数，笔数占比，金额占比，人数占比），理财商城（笔数，金额，人数，笔数占比，金额占比，人数占比）
     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('全部理财产品'
            ,(SELECT count(*) from TEMPDATA)
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA)
            ,(select count(distinct N3) from TEMPDATA)
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557'))
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA)
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903'))
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN N2 ELSE 0 END) FROM TEMPDATA)
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552'))
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA)
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905'))
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA)
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905'))
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('收益凭证'
            ,(SELECT count(*) from TEMPDATA where C1='1000008')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C1='1000008')
            ,(select count(distinct N3) from TEMPDATA where C1='1000008')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C1='1000008')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000008')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C1='1000008')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C1='1000008')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000008')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C1='1000008')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C1='1000008')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000008')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C1='1000008')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C1='1000008')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000008')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C1='1000008')
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('银行理财'
            ,(SELECT count(*) from TEMPDATA where C1='1000009' or C1='1000010')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C1='1000009' or C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where C1='1000009' or C1='1000010')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and (C1='1000009' or C1='1000010'))
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where (C1='1000009' or C1='1000010'))
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and (C1='1000009' or C1='1000010'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and (C1='1000009' or C1='1000010'))
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009' or C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and (C1='1000009' or C1='1000010'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and (C1='1000009' or C1='1000010'))
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009' or C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and (C1='1000009' or C1='1000010'))
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and (C1='1000009' or C1='1000010'))
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009' or C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and (C1='1000009' or C1='1000010'))
            );


     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('银行理财-南京银行'
            ,(SELECT count(*) from TEMPDATA where C1='1000010')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where C1='1000010')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C1='1000010')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C1='1000010')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C1='1000010')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C1='1000010')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C1='1000010')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN N2 ELSE 0 END) FROM TEMPDATA where C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C1='1000010')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C1='1000010')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN N2 ELSE 0 END) FROM TEMPDATA where C1='1000010')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C1='1000010')
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('银行理财-兴业银行'
            ,(SELECT count(*) from TEMPDATA where C1='1000009')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C1='1000009')
            ,(select count(distinct N3) from TEMPDATA where C1='1000009')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C1='1000009')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C1='1000009')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C1='1000009')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C1='1000009')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C1='1000009')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C1='1000009')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C1='1000009')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C1='1000009')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C1='1000009')
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金'
            ,(SELECT count(*) from TEMPDATA where C3='2')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2')
            ,(select count(distinct N3) from TEMPDATA where C3='2')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C3='2')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C3='2')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2')
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金-认购结果'
            ,(SELECT count(*) from TEMPDATA where C3='2' and C4 = '130')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2' and C4 = '130')
            ,(select count(distinct N3) from TEMPDATA where C3='2' and C4 = '130')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '130')
            ,(select SUM(CASE WHEN (C6 = '77775557')  THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '130')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '130')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '130')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '130')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '130')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '130')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '130')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '130')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '130')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '130')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '130')
            );

     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金-申购'
            ,(SELECT count(*) from TEMPDATA where C3='2' and C4 = '122')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2' and C4 = '122')
            ,(select count(distinct N3) from TEMPDATA where C3='2' and C4 = '122')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '122')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '122')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '122')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '122')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '122')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '122')
            ,(SELECT count(*) from TEMPDATA where(C6 = '77775552') and C3='2' and C4 = '122')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '122')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '122')
            ,(SELECT count(*) from TEMPDATA where(C6 = '99990905') and C3='2' and C4 = '122')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '122')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '122')
            );
     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金-定时定额投资'
            ,(SELECT count(*) from TEMPDATA where C3='2' and C4 = '139')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2' and C4 = '139')
            ,(select count(distinct N3) from TEMPDATA where C3='2' and C4 = '139')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '139')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '139')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '139')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '139')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '139')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '139')
            ,(SELECT count(*) from TEMPDATA where(C6 = '77775552') and C3='2' and C4 = '139')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '139')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '139')
            ,(SELECT count(*) from TEMPDATA where(C6 = '99990905') and C3='2' and C4 = '139')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '139')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '139')
            );
     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金-赎回'
            ,(SELECT count(*) from TEMPDATA where C3='2' and C4 = '124')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2' and C4 = '124')
            ,(select count(distinct N3) from TEMPDATA where C3='2' and C4 = '124')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '124')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '124')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '124')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '124')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '124')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '124')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '124')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '124')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '124')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '124')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '124')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '124')
            );
/*     INSERT INTO NJZQ_XTGL.TEMPDATA_BB(C1, C2, C3, C4, C5, C6, C7, N1, N2, N3, N7, N8, N9, N13, N14, N15)
     values('场外基金-强制赎回'
            ,(SELECT count(*) from TEMPDATA where C3='2' and C4 = '142')
            ,(select SUM(nvl(N2,0)) FROM TEMPDATA where C3='2' and C4 = '142')
            ,(select count(distinct N3) from TEMPDATA where C3='2' and C4 = '142')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '142')
            ,(select SUM(CASE WHEN (C6 = '77775557') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '142')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775557') and C3='2' and C4 = '142')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '142')
            ,(select SUM(CASE WHEN (C6 = '99991903') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '142')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99991903') and C3='2' and C4 = '142')
            ,(SELECT count(*) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '142')
            ,(select SUM(CASE WHEN (C6 = '77775552') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '142')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '77775552') and C3='2' and C4 = '142')
            ,(SELECT count(*) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '142')
            ,(select SUM(CASE WHEN (C6 = '99990905') THEN  N2 ELSE 0 END) FROM TEMPDATA where C3='2' and C4 = '142')
            ,(select count(distinct N3) from TEMPDATA where (C6 = '99990905') and C3='2' and C4 = '142')
            );*/
     --(C1, C2, C3, C4, C5, C6, C7, C8, C9, C10, N1, N2, N3, N4, N3, N6, N7, N8, N9, N10, N11, N12)
     --分类，总笔数，总金额，总人数，金罗盘（笔数，金额，人数，笔数占比，金额占比，人数占比），鑫易通（笔数，金额，人数，笔数占比，金额占比，人数占比），理财商城（笔数，金额，人数，笔数占比，金额占比，人数占比）
     update NJZQ_XTGL.TEMPDATA_BB
            set C8 = (CASE WHEN C2 = 0 THEN 0 ELSE 100*C5/C2 END)   --金罗盘
              , C9 = (CASE WHEN C3 = 0 THEN 0 ELSE 100*C6/C3 END)
              , C10 = (CASE WHEN C4 = 0 THEN 0 ELSE 100*C7/C4 END)
              , N4 = (CASE WHEN C2 = 0 THEN 0 ELSE 100*N1/C2 END)   --鑫易通
              , N5 = (CASE WHEN C3 = 0 THEN 0 ELSE 100*N2/C3 END)
              , N6 = (CASE WHEN C4 = 0 THEN 0 ELSE 100*N3/C4 END)
              , N10 = (CASE WHEN C2 = 0 THEN 0 ELSE 100*N7/C2 END)  --理财商城
              , N11 = (CASE WHEN C3 = 0 THEN 0 ELSE 100*N8/C3 END)
              , N12 = (CASE WHEN C4 = 0 THEN 0 ELSE 100*N9/C4 END)
              , N16 = (CASE WHEN C2 = 0 THEN 0 ELSE 100*N13/C2 END) --大智慧
              , N17 = (CASE WHEN C3 = 0 THEN 0 ELSE 100*N14/C3 END)
              , N18 = (CASE WHEN C4 = 0 THEN 0 ELSE 100*N15/C4 END)
              ;


    COMMIT;

    open cur_Result for
    select C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,N1,N2,N3,N4,N5,N6,N7,N8,N9,N10,N11,N12,N13,N14,N15,N16,N17,N18
    from NJZQ_XTGL.TEMPDATA_BB;

 exception
   when others then
    verrmsg:=sqlerrm;
      open cur_Result for select '网络金融部报表-销售情况统计报表-'||verrmsg 错误  from dual;


end PCX_NJZQ_XSQKTJBB;

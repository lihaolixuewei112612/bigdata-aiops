/*
    @@name:交割明细流水清洗转换程序
    @@componet_type: Inceptor
    @@author:李伟
    @@version: V1.0.0
    @@update_time:2019-11-18
    @@comment:EMC_APEX 修改：插入分区表时去除插入列名列表中的分区字段
    @@注意事项：
    依赖的表：DSC_BAS.T_ZQHQ_XZ_HIS  证券行情修正  DSC_BAS.T_JRCPHQ_HIS  金融产品行情
    INFO.TXGGL 新股管理   INFO.TGP_GPPG  股票配股  INFO.TGP_GSFHKG  分红扩股  INFO.TZQ_KZZ  可转债
    DSC_BAS.T_ZQYE_HIS  上日持仓信息
    修改记录
     ------------------------------------------------------------------------
       操作人      操作时间                   操作
       李伟        2018-12-17                     创建
       陈统        2019-07-24                   修改：调整了 交易类别为20, 21 成交编号分别为'内部转户'、'开户认领' 的成交金额 与应收金额
                                                修改：调整方式为 成交/应收金额 = 成交数量 * 最新价(没有最新价用昨收盘价格)
       陈统        2019-07-25                   修改：修改交易类别为 9，10，18，19，15的成交应收金额，修改方式同上
                                                      其中,成交编号为 "非流通转出表" 的操作,由于未对流通持仓产生影响,且非流通持仓有变化,未作修改
       陈统        2019-07-26                   修改：调整了 交易类别为31 成交编号为 非交易过户
       陈统        2019-08-01                   修改：调整了 emc_pub.t_param_value 中 param_value 字段的值; -- DSC_CFG.T_ZQPZ_DY
                                                                                                    -> DSC_CFG.T_ZQPZ_DY_BAK
       陈统        2019-09-04                   修改：调整集中交易 配股 应收数量操作
       陈统        2019-09-05                   修改：调整了 交易类别为12 成交编号分别为'回售转出' 的成交金额
       陈统        2019-09-06                   修改：修正最新价，根据JJJYBZ 判断ZXJ 是否需要加上zxlx
       李伟        2019-09-25                   修改：新股和可转债网申成交金额和应收金额的修复
       李伟        2019-09-29                   修改：非流通、股份转让以及上海可转债不行权的成交金额、应收金额以及应收数量
       李伟        2019-10-28                   修改：可转债申购修规有交易类别21(调帐转出)进行清除全体持仓，cjsl和yssl修正为0
       李伟        2019-11-05                   修改：CJBH为非流通股转入，通过yssl修正成交数量和成交金额，算转入市值
       张伟真      2019-11-17                   修改：上海ETF认购上市修正CJJE
       燕居庆      2019-12-13                   修改：CJBH为股份转让，JYLB=18，修正成交金额和应收金额
       燕居庆      2019-12-17                   修改：CJBH为非流通股转出，通过YSSL修正成交数量和成交金额，算转出市值
       燕居庆      2020-01-06                   修改：CJBH为清除帐户持仓，修正应收金额
       燕居庆      2020-01-14                   修改：交易类别为65/66，修正成交金额
       杨启旺      2020-03-24                   修改：取消可转债SSRQ=0的过滤，针对SSRQ=0为3000000
       燕居庆      2020-04-20                   修改：JYLB=19&&CJBH=‘ETF转出’ 修正应收数量为0
       燕居庆      2020-07-03                   修改：JYLB='20' AND CJBH='非流通股转入' 非流通股分红扩股产生的市值转入，cjje和ysje无需修正
       燕居庆      2020-07-03                   修改：JYLB='19'&&ZQLB IN ('AC','AD'),针对A股退市的cjje和ysje进行修正
	   李伟      2020-07-28                   		   修改：20200722日起有上海科创板lof认购业务：需要对科创板lof认购时的jylb='18'不能算市值转入，cjje、yssl以及ysje修复为0
	         JYLB='18' AND SUBSTR(TRIM(ZQLB),1,1)='L' AND CJBH='份额变动' AND JYS='2' AND SUBSTR(TRIM(ZQDM),1,4)='5060'
     林娴    2021-01-21           修改：港股股份合并，修正生效日期的yssl=0

*/

----定义变量
--设置空字符串过滤
set character.literal.as.string=true;

--T-1日
SET hivevar:VN_T_DATE = F_GET_JYR_DATE(${hivevar:RQ},-1);

-----STEP0:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_01;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_02;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_03;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_01_1;
DROP TABLE IF EXISTS TEMP.TEMP_JGMXLS_QYDM;

-----STEP1:创建临时表，并将SRC层的交割流水数据进行清洗转换生成进去
CREATE TABLE TEMP.TEMP_T_JGMXLS_HIS_01
AS
SELECT * FROM DSC_BAS.T_JGMXLS_HIS WHERE 1=0;

INSERT INTO TEMP.TEMP_T_JGMXLS_HIS_01
    (SEQNO,
     LSH,
     JGZLLB,
     WTH,
     KHH,
     KHXM,
     GDH,
     GDXM,
     JYS,
     JSLX,
     JSJG,
     JSZH,
     BZ,
     WTFS,
     YYB,
     XWDM,
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
     BZS1,
     S1,
     S2,
     S3,
     S4,
     S5,
     S6,
     YSJE,
     S11,
     S12,
     S13,
     S15,
     S16,
     YSJE_YJ,
     BCYE,
     ZJMX_LSH,
     DJJE,
     YSSL,
     BCSL,
     BCDJSL,
     DJSL,
     BCYK,
     SETTLE_DATE,
     RZRQ,
     FSYYB,
     LOGINID,
     FHGY,
     JYJ,
     YYS)
    SELECT SEQNO,
           LSH,
           JGZLLB,
           WTH,
           KHH,
           KHXM,
           GDH,
           GDXM,
           T1.JYS,
           JSLX,
           JSJG,
           JSZH,
           T1.BZ,
           WTFS,
           T1.YYB,
           XWDM,
           T1.ZQDM,
           ZQMC,
           T1.ZQLB,
           JYLB,
           CJBH,
           CJRQ,
           CJSJ,
           SBSJ,
           CJBS,
           CASE
                -- 20191028 可转债认购代码清仓证券数量修复
                WHEN T1.JYLB ='21' AND substr(T1.ZQLB,1,1) = 'Z' AND T1.cjbh='清除全体持仓' AND T1.CJSL <> 0 THEN
                    0
                /*-- 20191106 非流通股转入 成交数量修正
                WHEN T1.JYLB ='20' AND T1.CJBH = '非流通股转入' AND T1.CJSL = 0 THEN
                    ABS(NVL(T1.YSSL,0))*/
                WHEN T1.JYLB IN ('21') AND T1.ZQLB = 'AJ' THEN
                    0
                ELSE
                    CJSL
            END * NVL(T2.JYDW, 1) AS CJSL,
           CJJG,
           JSJ,
           LXJG,
           CAST(CASE
            WHEN T1.JYLB IN ('7') AND T1.CJBH = '转托管转出' THEN NVL2(NULLIF(T1.CJJE,0),T1.CJJE,
                                                            (CJSL * (CASE WHEN NVL(ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('9') AND T1.CJBH = '指定余额' THEN NVL2(NULLIF(T1.CJJE,0),T1.CJJE,
                                                            (CJSL * (CASE WHEN NVL(ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END))) -- 此处强转与0比较不可行，只能用NULLIF函数比较返回NULL
            WHEN T1.JYLB IN ('10') AND T1.CJBH = '转出市值' THEN NVL2(NULLIF(T1.CJJE,0),T1.CJJE,
                                                            (CJSL * (CASE WHEN NVL(ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('12') AND T1.CJBH = '回售转出' THEN NVL2(NULLIF(T1.CJJE,0),T1.CJJE,
                                                            (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('15') AND T1.CJBH = '转托管转入' THEN NVL2(NULLIF(T1.CJJE,0),T1.CJJE,
                                                            (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))
            -- OR T1.CJBH = '上市流通'  20190929 调整右括号，包含现有多种CJBH的情况
			-- 20200728 上海科创板lof认购
			WHEN T1.JYLB='18' AND SUBSTR(TRIM(T1.ZQLB),1,1)='L' AND T1.CJBH='份额变动' AND T1.JYS='2' AND SUBSTR(TRIM(T1.ZQDM),1,4)='5060' THEN 0
            WHEN T1.JYLB IN ('18') AND (T1.CJBH = '确权转入' OR T1.CJBH ='转流通股'
            OR (T1.CJBH = '上市流通' AND T1.JYS = '2' AND T1.ZQLB = 'A0' AND (E.GPDM IS NULL) AND (F.ZQDM IS NULL) AND (G.ZQDM IS NULL))
            OR (T1.CJBH = '上市流通' AND T1.JYS = '2' AND T1.ZQLB = 'L0')           -- 这里可以优化
            OR (T1.CJBH = '股份转让' AND T1.JYS = '1')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '5')
            OR (T1.CJBH = '期权行权' /*AND T1.JYS = '2' */ AND T1.ZQLB = 'E0') or
            --OR (T1.CJBH = '上市流通' AND T1.JYS = '2' AND T1.ZQLB = 'E0') 20200731新增份额变动
            (T1.CJBH = '份额变动' AND T1.JYS = '2' AND T1.ZQLB = 'L0')
			)  -- 上海ETF认购上市
                THEN NVL2(NULLIF(T1.CJJE,0), T1.CJJE,(T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            -- 其中上市流通的CJBH的情况 当 不是 上海配售和新股申购 时修改成交金额
            -- WHEN T1.JYLB IN ('18') AND (T1.CJBH = '上市流通' AND ((F.GPDM IS NOT NULL) OR (E.ZQDM IS NOT NULL))) THEN 0

            -- OR T1.CJBH = '份额变动' T1.CJBH = '调整变动'  OR
            WHEN T1.JYLB IN ('19') AND ((T1.CJBH = '股份托管')
            OR (T1.CJBH = '期权行权' AND T1.JYS = '2' AND T1.ZQLB = 'E0')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '1')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '5')
            OR (T1.CJBH = '退出结算' AND T1.JYS = '5' AND T1.ZQLB = 'AJ')
			-- 20200731 新增股份转让
			OR (T1.CJBH = '股份转让' AND T1.JYS = '1' AND T1.ZQLB = 'A0')
            OR (T1.ZQLB IN ('AC' ,'AD')) --20020703 A股退市
            --OR (T1.CJBH = '上市转出' AND T1.JYS = '2' AND T1.ZQLB = 'A3' AND T1.ZQDM LIKE '5%') -- 上海ETF上市的认购代码转出
            ) THEN
                            NVL2(NULLIF(T1.CJJE,0),T1.CJJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('20') AND (T1.CJBH = '开户认领' OR T1.CJBH = '非流通转入' OR T1.CJBH = '手工调帐转入') THEN
            --OR (T1.CJBH = '手工调帐转入' AND T1.JYS = '2' AND T1.ZQLB = 'L0')
                        NVL2(NULLIF(T1.CJJE,0),T1.CJJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN NVL(D.ZSP,H.ZXJZ) ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('21') AND (T1.CJBH = '内部转户' OR T1.CJBH = '手工调帐转出' OR T1.CJBH = '清除帐户持仓') THEN
                            NVL2(NULLIF(T1.CJJE,0),T1.CJJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('31') AND (T1.CJBH = '非交易过户'  OR T1.CJBH = '转融通过户:113')THEN
                            NVL2(NULLIF(T1.CJJE,0),T1.CJJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('65','66') AND T1.JYS IN ('2', '1') THEN
                            NVL2(NULLIF(T1.CJJE,0),T1.CJJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))
                            -- 深证 65担保物提交 66担保物返回 只做成交金额的修正
            -- 20191106 非流通股转入 成交金额修正
            WHEN T1.JYLB ='20' AND T1.CJBH = '非流通股转入' AND NVL(T1.CJJE,0) = 0 THEN
                    ABS(NVL(T1.YSSL,0))* (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)
            ELSE T1.CJJE
           END AS DECIMAL(16,2)) AS CJJE,
           LXJE,
           BZS1,
           S1,
           S2,
           S3,
           S4,
           S5,
           S6,
           CAST(CASE
            WHEN T1.JYLB IN ('7') AND T1.CJBH = '转托管转出' THEN NVL2(NULLIF(T1.YSJE,0),T1.YSJE,
                                                            (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('9') AND T1.CJBH = '指定余额' THEN NVL2(NULLIF(T1.YSJE,0),T1.YSJE,
                                                            (0-(T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END))))

            WHEN T1.JYLB IN ('10') AND T1.CJBH = '转出市值' THEN NVL2(NULLIF(T1.YSJE,0),T1.YSJE,
                                                            (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('15') AND T1.CJBH = '转托管转入' THEN NVL2(NULLIF(T1.YSJE,0),T1.YSJE,
                                                            (0-(T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END))))
            -- 20200728 上海科创板lof认购
			WHEN T1.JYLB='18' AND SUBSTR(TRIM(T1.ZQLB),1,1)='L' AND T1.CJBH='份额变动' AND T1.JYS='2' AND SUBSTR(TRIM(T1.ZQDM),1,4)='5060' THEN
			   0
            -- OR T1.CJBH = '上市流通'  20190929 调整右括号，包含现有多种CJBH的情况
            WHEN T1.JYLB IN ('18') AND (T1.CJBH = '确权转入' OR T1.CJBH ='转流通股'
            OR (T1.CJBH = '上市流通' AND T1.JYS = '2' AND T1.ZQLB = 'A0' AND (E.GPDM IS NULL) AND (F.ZQDM IS NULL) AND (G.ZQDM IS NULL))
            OR (T1.CJBH = '上市流通' AND T1.JYS = '2' AND T1.ZQLB = 'L0')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '1')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '5')
            OR (T1.CJBH = '期权行权' /*AND T1.JYS = '2' */AND T1.ZQLB = 'E0'))
                THEN NVL2(NULLIF(T1.YSJE,0), T1.YSJE,(T1.CJSL *(0-(CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END))))
            -- 其中上市流通的CJBH的情况 当 不是 上海配售和新股申购 时修改应收金额
            -- WHEN T1.JYLB IN ('18') AND (T1.CJBH = '上市流通' AND ((F.GPDM IS NULL) OR (E.ZQDM IS NULL))) THEN 0

            -- OR T1.CJBH = '份额变动' T1.CJBH = '调整变动' OR
            WHEN T1.JYLB IN ('19') AND ((T1.CJBH = '股份托管')
            OR (T1.CJBH = '期权行权' AND /*T1.JYS = '2' AND*/ T1.ZQLB = 'E0')
            OR (T1.CJBH = '股份转让' AND T1.JYS = '1' AND T1.ZQLB = 'A0')
			--20200731 新增zqlb='A0'
            OR (T1.CJBH = '股份转让' AND T1.JYS = '5')
            OR (T1.CJBH = '退出结算' AND T1.JYS = '5' AND T1.ZQLB = 'AJ')
            OR (T1.ZQLB IN ('AC' ,'AD')) --20200703 A股退市
            ) THEN
                            NVL2(NULLIF(T1.YSJE,0),T1.YSJE,(T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))
            WHEN T1.JYLB IN ('20') AND (T1.CJBH = '开户认领' OR T1.CJBH = '手工调帐转入') THEN
            -- AND T1.JYS = '2' AND T1.ZQLB = 'L0' 目前发现手工调账转入都是要修正的,但是没有市值
                NVL2(NULLIF(T1.YSJE,0),T1.YSJE,(T1.CJSL * (0-(CASE WHEN NVL(D.ZXJ,0)=0 THEN NVL(D.ZSP,H.ZXJZ) ELSE D.ZXJ END))))

            WHEN T1.JYLB IN ('21') AND (T1.CJBH = '内部转户' OR T1.CJBH = '手工调帐转出' OR T1.CJBH='清除帐户持仓')THEN
                            NVL2(NULLIF(T1.YSJE,0),T1.YSJE,(T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))

            WHEN T1.JYLB IN ('31') AND T1.CJBH = '非交易过户' THEN
                            NVL2(NULLIF(T1.YSJE,0),T1.YSJE, (T1.CJSL * (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)))
            WHEN T1.JYLB ='20' AND T1.CJBH = '非流通股转入' AND NVL(T1.YSJE,0) = 0 THEN
                    (NVL(T1.YSSL,0))* (CASE WHEN NVL(D.ZXJ,0)=0 THEN D.ZSP ELSE D.ZXJ END)
            ELSE T1.YSJE
           END  AS DECIMAL(16,2)) AS YSJE,
           S11,
           S12,
           S13,
           S15,
           S16,
           YSJE_YJ,
           BCYE,
           ZJMX_LSH,
           DJJE,
           CASE
            WHEN T1.JYLB IN ('1') AND T1.YSSL = 0 THEN
                CJSL
            WHEN T1.JYLB IN ('2') AND T1.YSSL = 0 THEN
                (0 - CJSL)
            WHEN T1.JYLB IN ('18') AND T1.ZQLB = 'A0' AND T1.CJBH = '权益转入' THEN
                0
            WHEN T1.JYLB IN ('3')  AND T1.ZQLB = 'A9' AND T1.CJBH = '配债认购' THEN
                CJSL
            WHEN T1.JYLB IN ('19')  AND T1.ZQLB = 'E0' AND T1.CJBH = 'ETF转出' THEN
                0
            WHEN T1.JYLB IN ('19')  AND T1.ZQLB = 'A9' AND T1.CJBH = '退出结算' THEN
                0
            -- 上海LOF申购/赎回存在18/19与之相对应，42/43的流水YSSL均为0，考虑使用CJSL进行修正。
            WHEN T1.JYLB IN ('42')  AND T1.ZQLB = 'L0' AND T1.JYS = '2' THEN
                CJSL
            WHEN T1.JYLB IN ('43')  AND T1.ZQLB = 'L0' AND T1.JYS = '2' THEN
                0 - CJSL
			-- 20200728 上海科创板lof认购
			WHEN T1.JYLB='18' AND SUBSTR(TRIM(T1.ZQLB),1,1)='L' AND T1.CJBH='份额变动' AND T1.JYS='2' AND SUBSTR(TRIM(T1.ZQDM),1,4)='5060' THEN
			   0
            -- 上海可转债(行权)
            WHEN T1.JYLB IN ('18')  AND T1.ZQLB = 'Z6' AND T1.JYS = '2' AND (T1.CJBH = '收益结转' OR T1.CJBH = '非交易过户') THEN
                0
            WHEN T1.JYLB IN ('3')   AND T1.ZQLB = 'Z6' AND T1.JYS = '2' THEN
                CJSL
            -- 沪深可转债申购
            WHEN T1.JYLB IN ('19')  AND T1.ZQLB = 'ZD' AND (T1.CJBH = '转债中签通知转出' or T1.CJBH = '中签通知转出' ) THEN
                0
            -- 上海配股
            WHEN T1.JYLB IN ('19')  AND T1.ZQLB = 'A1' AND T1.CJBH = '收益结转' AND T1.JYS = '2' THEN
                0
            WHEN T1.JYLB IN ('3')   AND T1.ZQLB = 'A1' AND T1.JYS = '2' THEN
                CJSL
            -- 上海可转债不行权 20190929 ???行权是否存在CJBH为null的情况??为null的情况???  确认这个成交编号为
            WHEN T1.JYLB IN ('18')  AND T1.ZQLB = 'Z6' AND T1.JYS = '2' AND (NVL(T1.CJBH,'1') = '1' OR T1.CJBH = '') THEN
                0
            WHEN T1.JYLB IN ('19')  AND T1.ZQLB = 'Z6' AND T1.JYS = '2' AND (NVL(T1.CJBH,'1') = '1' OR T1.CJBH = '') THEN
                0
            -- 20191028 可转债认购代码清仓应收数量修复
            WHEN T1.JYLB ='21' AND T1.ZQLB = 'ZD' AND T1.CJBH='清除全体持仓' AND T1.YSSL <> 0  THEN
                0
             -- 20210121 港股股份合并，修正生效日期的yssl=0
            WHEN T1.JYS in ('8','9') and T1.JYLB = '19' and T1.CJBH = '代码转换' THEN
                0
            ELSE
                YSSL
           END * NVL(T2.JYDW, 1),
           BCSL * NVL(T2.JYDW, 1),
           BCDJSL * NVL(T2.JYDW, 1),
           DJSL * NVL(T2.JYDW, 1),
           BCYK,
           SETTLE_DATE,
           RZRQ,
           FSYYB,
           LOGINID,
           FHGY,
           CAST(ROUND(CASE
               WHEN T1.JYS IN ('8', '9') THEN
                S1
               ELSE
                S1 + S3 - (S11 + S12 + S13 + S16)
           END,2) AS DECIMAL(16,2)) AS JYJ,
           CAST(ROUND((CASE
               WHEN T1.JYS IN ('8', '9') THEN
                S1
               ELSE
                S1 + S3 - (S11 + S12 + S13 + S16)
           END) * NVL(B.SL, C.SL),6) AS DECIMAL(22,6)) AS YYS
      FROM (SELECT SEQNO,
                   LSH,
                   CAST(JGZLLB AS STRING) AS JGZLLB,--交割资料类别
                   WTH,
                   KHH,
                   KHXM,
                   GDH,
                   GDXM,
                   F_GET_ETL_TRAN_DICVAL('DSC_BAS',
                                              'T_JGMXLS_HIS',
                                              'JYS',
                                              1,
                                              1,
                                              A.JYS) AS JYS,
                   CAST(JSLX AS STRING) AS JSLX,
                   JSJG,  --结算机构
                   JSZH,  --结算账号
                   F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_JGMXLS_HIS', 'BZ', 1, 1, A.BZ) AS BZ,--币种
                   CAST(WTFS AS STRING) AS WTFS,  --委托方式
                   F_GET_NEW_YYB(A.YYB, '') AS YYB,  --营业部
                   XWDM,   --席位代码
                   ZQDM,   --证券代码
                   ZQMC,   --证券名称
                   ZQLB,
                   CAST(WTLB AS STRING) AS JYLB,
                   --'21' AS JYLB,
                   --'内部转户' AS CJBH,
                   CJBH,
                   CJRQ,
                   CJSJ,
                   SBSJ,
                   CJBS,
                   --1200 AS CJSL,
                   CJSL,
                   CJJG,
                   JSJ,
                   LXJG,--利息价格
                   --4752 AS CJJE,
                   CJJE,--利息金额
                   LXJE,
                   BZS1,
                   S1,
                   S2,
                   S3,
                   S4,
                   S5,
                   S6,
                   --0 AS YSJE,         -- 如果交易类别为 21 zc 那么，YSJE要有值 = 数量*行情
                   YSJE,            -- 如果交易类别为 21 zc 那么，YSJE要有值 = 数量*行情
                   S11,
                   S12,
                   S13,
                   S15,
                   S16,
                   YSJE_YJ,
                   BCYE,
                   ZJMX_LSH,--资金明细流水号
                   DJJE,
                   --1200 AS YSSL,
                   YSSL,
                   BCSL,--本次股份余额
                   BCDJSL,--本次股份冻结余额
                   DJSL,--冻结数量
                   BCYK,--本次盈亏
                   JSRQ AS SETTLE_DATE, --交收日期
                   RZRQ,  --入账日期
                   FSYYB,  --发生营业部
                   LOGINID,--操作人登录ID
                   FHGY    --复核柜员
              FROM SRC_ABOSS.TJGMXLS A
             WHERE CJRQ = ${hivevar:RQ}) T1
      LEFT JOIN DSC_CFG.T_ZQLB T2
        ON T1.JYS = T2.JYS
       AND T1.ZQLB = T2.ZQLB
      LEFT JOIN (SELECT YYB, SL
                   FROM DSC_CFG.T_YYSLCS
                  WHERE KSRQ <= ${hivevar:RQ}
                    AND JSRQ >= ${hivevar:RQ}) B
        ON T1.YYB = B.YYB
      JOIN (SELECT YYB, SL, ${hivevar:RQ} AS RQ
              FROM DSC_CFG.T_YYSLCS
             WHERE KSRQ <= ${hivevar:RQ}
               AND JSRQ >= ${hivevar:RQ}
               AND YYB = '1') C
        ON T1.CJRQ = C.RQ
      LEFT JOIN(SELECT
                    CASE
                     WHEN JJJYBZ = 1 THEN
                        (NVL(ZXJ, 0) + NVL(LXJG, 0))
                     ELSE
                        ZXJ
                    END * NVL(JYDW, 1) AS ZXJ,
                    ZSP * NVL(JYDW, 1) AS ZSP,
                    JYS,
                    ZQDM
                   FROM DSC_BAS.T_ZQHQ_XZ_HIS
                   WHERE RQ = ${hivevar:RQ})    D       -- 从这张证券行情表里面读取最新价格
        ON (T1.JYS = D.JYS AND T1.ZQDM = D.ZQDM)

      LEFT JOIN (SELECT JYS,GPDM,F_GET_JYR_DATE(SSRQ, -1) AS RQ
                   FROM INFO.TXGGL WHERE JYS = '2') E
        ON (T1.JYS = E.JYS
            AND T1.ZQDM = E.GPDM
            AND E.RQ = T1.CJRQ)
      LEFT JOIN (SELECT JYS,ZQDM,F_GET_JYR_DATE(PGSSRQ, -1) AS RQ
                   FROM INFO.TGP_GPPG WHERE JYS = '2') F
        ON (T1.JYS = F.JYS
            AND T1.ZQDM = F.ZQDM
            AND F.RQ = T1.CJRQ)
      LEFT JOIN (SELECT JYS,ZQDM,CQCXRQ AS RQ
                   FROM INFO.TGP_GSFHKG WHERE JYS = '2') G
        ON (T1.JYS = G.JYS
            AND T1.ZQDM = G.ZQDM
            AND G.RQ = T1.CJRQ)
      LEFT JOIN (SELECT CPDM,zxjz
                    FROM DSC_BAS.T_JRCPHQ_HIS
                WHERE RQ = ${hivevar:RQ}) H                     -- 这边关联场外的行情表
        ON (T1.ZQDM = H.CPDM);

-- 20190925 新股和可转债网上申购，成交金额和应收金额进行处理
CREATE TABLE TEMP.TEMP_JGMXLS_QYDM
AS
SELECT X.JYS, X.GPDM AS ZQDM, SGDM AS QYDM, 'XG' AS QYLX
  FROM INFO.TXGGL X
 WHERE X.ZQGBR IS NOT NULL
   AND X.ZQGBR <> 0
   --AND X.SSRQ IS NOT NULL
   --AND X.SSRQ <> 0
   AND X.GPDM IS NOT NULL
   AND X.GPDM <> ''
   AND X.SGDM IS NOT NULL
   AND X.SGDM <> ''
   AND ${hivevar:RQ} BETWEEN X.ZQGBR AND F_GET_JYR_DATE(IF(NVL(X.SSRQ, 0) =0,30000000,X.SSRQ),-1)
UNION ALL
SELECT CASE
           WHEN SUBSTR(Z.GPDM, 1, 1) = '6' THEN
            '2' --上海
           WHEN SUBSTR(Z.GPDM, 1, 1) IN ('0', '3') THEN
            '1' --深圳
           ELSE
            NULL
       END AS JYS,
       ZQDM,
       SGDM AS QYDM,
       'KZZ_SG' AS QYLX
  FROM INFO.TZQ_KZZ Z
 WHERE Z.ZQDM IS NOT NULL
   AND Z.ZQDM <> ''
   AND Z.SGDM IS NOT NULL
   AND Z.SGDM <> ''
   AND Z.ZQHGBR IS NOT NULL
   AND Z.ZQHGBR <> 0
   --AND Z.SSRQ IS NOT NULL
   --AND Z.SSRQ <> 0
   AND ${hivevar:RQ} BETWEEN Z.ZQHGBR AND F_GET_JYR_DATE(IF(NVL(Z.SSRQ, 0) =0,30000000,Z.SSRQ),-1)
UNION ALL
SELECT CASE
           WHEN SUBSTR(Z.GPDM, 1, 1) = '6' THEN
            '2' --上海
           WHEN SUBSTR(Z.GPDM, 1, 1) IN ('0', '3') THEN
            '1' --深圳
           ELSE
            NULL
       END AS JYS,
       ZQDM,
       RGDM AS QYDM,
       'KZZ_RG_SH' AS QYLX
  FROM INFO.TZQ_KZZ Z
 WHERE SUBSTR(Z.GPDM, 1, 1) = '6' --上海可转债认购
   AND Z.ZQDM IS NOT NULL
   AND Z.ZQDM <> ''
   AND Z.RGDM IS NOT NULL
   AND Z.RGDM <> ''
   AND Z.ZQHGBR IS NOT NULL
   AND Z.ZQHGBR <> 0
   --AND Z.SSRQ IS NOT NULL
   --AND Z.SSRQ <> 0
   AND ${hivevar:RQ} BETWEEN Z.ZQHGBR AND F_GET_JYR_DATE(IF(NVL(Z.SSRQ, 0) =0,30000000,Z.SSRQ),-1);


CREATE TABLE TEMP.TEMP_T_JGMXLS_HIS_01_1
AS
SELECT * FROM DSC_BAS.T_JGMXLS_HIS
WHERE 1= 2;

INSERT INTO TEMP.TEMP_T_JGMXLS_HIS_01_1
    (SEQNO,
     LSH,
     JGZLLB,
     WTH,
     KHH,
     KHXM,
     GDH,
     GDXM,
     JYS,
     JSLX,
     JSJG,
     JSZH,
     BZ,
     WTFS,
     YYB,
     XWDM,
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
     BZS1,
     S1,
     S2,
     S3,
     S4,
     S5,
     S6,
     YSJE,
     S11,
     S12,
     S13,
     S15,
     S16,
     YSJE_YJ,
     BCYE,
     ZJMX_LSH,
     DJJE,
     YSSL,
     BCSL,
     BCDJSL,
     DJSL,
     BCYK,
     SETTLE_DATE,
     RZRQ,
     FSYYB,
     LOGINID,
     FHGY,
     JYJ,
     YYS)
    SELECT T.SEQNO,
           T.LSH,
           T.JGZLLB,
           T.WTH,
           T.KHH,
           T.KHXM,
           T.GDH,
           T.GDXM,
           T.JYS,
           T.JSLX,
           T.JSJG,
           T.JSZH,
           T.BZ,
           T.WTFS,
           T.YYB,
           T.XWDM,
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
           CASE
               WHEN T.JYLB = '18' AND T.CJBH = '新股上市' AND T.JYS = '1' AND T.CJSL <> 0 AND
                    T.CJJE = 0 AND QY.QYLX = 'XG' AND A.KHH IS NULL --深圳新股网下申购
                THEN
                CAST(ROUND(T.CJSL * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH = '上市流通' AND T.JYS = '2' AND T.CJSL <> 0 AND
                    T.CJJE = 0 AND QY.QYLX = 'XG' AND A.KHH IS NULL --上海新股网下申购
                THEN
                CAST(ROUND(T.CJSL * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH IN ('新股上市') AND T.JYS = '1' AND T.CJSL <> 0 AND
                    T.CJJE = 0 AND QY.QYLX = 'KZZ_SG' AND A.KHH IS NULL --深圳可转债网下申购
                THEN
                CAST(ROUND(T.CJSL * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH = '配债上市' AND T.JYS = '2' AND T.CJSL <> 0 AND
                    T.CJJE = 0 AND QY.QYLX = 'KZZ_SG' AND A.KHH IS NULL --上海可转债网下申购
                THEN
                CAST(ROUND(T.CJSL * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               ELSE
                T.CJJE
           END AS CJJE,
           T.LXJE,
           T.BZS1,
           T.S1,
           T.S2,
           T.S3,
           T.S4,
           T.S5,
           T.S6,
           CASE
               WHEN T.JYLB = '18' AND T.CJBH = '新股上市' AND T.JYS = '1' AND T.YSSL <> 0 AND
                    T.YSJE = 0 AND QY.QYLX = 'XG' AND A.KHH IS NULL --深圳新股网下申购
                THEN
                CAST(ROUND((-1) * ABS(T.YSSL) * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH = '上市流通' AND T.JYS = '2' AND T.YSSL <> 0 AND
                    T.YSJE = 0 AND QY.QYLX = 'XG' AND A.KHH IS NULL --上海新股网下申购
                THEN
                CAST(ROUND((-1) * ABS(T.YSSL) * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH IN ('新股上市') AND T.JYS = '1' AND T.YSSL <> 0 AND
                    T.YSJE = 0 AND QY.QYLX = 'KZZ_SG' AND A.KHH IS NULL --深圳可转债网下申购
                THEN
                CAST(ROUND((-1) * ABS(T.YSSL) * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               WHEN T.JYLB = '18' AND T.CJBH = '配债上市' AND T.JYS = '2' AND T.YSSL <> 0 AND
                    T.YSJE = 0 AND QY.QYLX = 'KZZ_SG' AND A.KHH IS NULL --上海可转债网下申购
                THEN
                CAST(ROUND((-1) * ABS(T.YSSL) * (CASE
                               WHEN NVL(HQ.JJJYBZ, 0) = 1 THEN
                                (NVL(HQ.ZXJ, 0) + NVL(HQ.LXJG, 0))
                               ELSE
                                NVL(HQ.ZXJ, 0)
                           END),
                           2) AS DECIMAL(16, 2))
               ELSE
                T.YSJE
           END AS YSJE,
           T.S11,
           T.S12,
           T.S13,
           T.S15,
           T.S16,
           T.YSJE_YJ,
           T.BCYE,
           T.ZJMX_LSH,
           T.DJJE,
           T.YSSL,
           T.BCSL,
           T.BCDJSL,
           T.DJSL,
           T.BCYK,
           T.SETTLE_DATE,
           T.RZRQ,
           T.FSYYB,
           T.LOGINID,
           T.FHGY,
           T.JYJ,
           T.YYS
      FROM TEMP.TEMP_T_JGMXLS_HIS_01 T
      LEFT JOIN (SELECT A.KHH, A.GDH, A.JYS, A.ZQSL, B.ZQDM --获取权益代码对应的正股代码
                   FROM DSC_BAS.T_ZQYE_HIS A, TEMP.TEMP_JGMXLS_QYDM B
                  WHERE A.RQ = ${hivevar:VN_T_DATE} --上个交易日
                    AND A.JYS = B.JYS
                    AND A.ZQDM = B.QYDM) A
        ON T.KHH = A.KHH
       AND T.GDH = A.GDH
       AND T.JYS = A.JYS
       AND T.ZQDM = A.ZQDM
       AND T.CJSL = A.ZQSL
      LEFT JOIN TEMP.TEMP_JGMXLS_QYDM QY
        ON QY.QYLX IN ('XG', 'KZZ_SG')
       AND T.JYS = QY.JYS
       AND T.ZQDM = QY.ZQDM
      LEFT JOIN DSC_BAS.T_ZQHQ_XZ_HIS HQ
        ON HQ.RQ = ${hivevar:RQ}
       AND T.JYS = HQ.JYS
       AND T.ZQDM = HQ.ZQDM
     WHERE T.CJRQ = ${hivevar:RQ};

-----STEP2：证券品种处理
-----STEP2.1：排除中小板
CREATE TABLE TEMP.TEMP_T_JGMXLS_HIS_02
AS
SELECT T.SEQNO,
       T.LSH,
       T.JGZLLB,
       T.WTH,
       T.KHH,
       T.KHXM,
       T.GDH,
       T.GDXM,
       T.JYS,
       T.JSLX,
       T.JSJG,
       T.JSZH,
       T.BZ,
       T.WTFS,
       T.YYB,
       T.XWDM,
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
       T.BZS1,
       T.S1,
       T.S2,
       T.S3,
       T.S4,
       T.S5,
       T.S6,
       T.YSJE,
       T.S11,
       T.S12,
       T.S13,
       T.S15,
       T.S16,
       T.YSJE_YJ,
       T.BCYE,
       T.ZJMX_LSH,
       T.DJJE,
       T.YSSL,
       T.BCSL,
       T.BCDJSL,
       T.DJSL,
       T.BCYK,
       T.SETTLE_DATE,
       T.RZRQ,
       T.FSYYB,
       T.LOGINID,
       T.FHGY,
       T.JYJ,
       T.YYS,
       CASE
           WHEN NOT (T.ZQDM LIKE '002%' AND T.ZQLB = 'A0' AND T.JYS = '1') THEN
            T1.ZQPZ
       END AS ZQPZ
  FROM TEMP.TEMP_T_JGMXLS_HIS_01_1 T
  LEFT JOIN (SELECT DISTINCT JYS, ZQLB, JB3_ZQPZ AS ZQPZ
               FROM DSC_CFG.T_ZQPZ_DY D                     -- DSC_CFG.T_ZQPZ_DY -> DSC_CFG.T_ZQPZ_DY_BAK
              WHERE D.JB3_ZQPZ <> '1102'                        -- 证券品种不为 1102
                AND IS_VALID = 1) T1
    ON (T.JYS = T1.JYS AND T.ZQLB = T1.ZQLB);

-----STEP2.2：处理中小板和基金部分
CREATE TABLE TEMP.TEMP_T_JGMXLS_HIS_03
AS
SELECT T.SEQNO,
       T.LSH,
       T.JGZLLB,
       T.WTH,
       T.KHH,
       T.KHXM,
       T.GDH,
       T.GDXM,
       T.JYS,
       T.JSLX,
       T.JSJG,
       T.JSZH,
       T.BZ,
       T.WTFS,
       T.YYB,
       T.XWDM,
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
       T.BZS1,
       T.S1,
       T.S2,
       T.S3,
       T.S4,
       T.S5,
       T.S6,
       T.YSJE,
       T.S11,
       T.S12,
       T.S13,
       T.S15,
       T.S16,
       T.YSJE_YJ,
       T.BCYE,
       T.ZJMX_LSH,
       T.DJJE,
       T.YSSL,
       T.BCSL,
       T.BCDJSL,
       T.DJSL,
       T.BCYK,
       T.SETTLE_DATE,
       T.RZRQ,
       T.FSYYB,
       T.LOGINID,
       T.FHGY,
       T.JYJ,
       T.YYS,
       CASE
           WHEN T.ZQDM LIKE '002%' AND T.ZQLB = 'A0' AND T.JYS = '1' THEN
            T1.ZQPZ
           WHEN T.ZQPZ = '2422' THEN            -- 2422 = 普通ETF
            NVL(T2.ZQPZ, '2422')
           ELSE
            T.ZQPZ
       END AS ZQPZ
  FROM TEMP.TEMP_T_JGMXLS_HIS_02 T
  LEFT JOIN (SELECT DISTINCT JYS, ZQLB, JB3_ZQPZ AS ZQPZ
               FROM DSC_CFG.T_ZQPZ_DY D
              WHERE D.JB3_ZQPZ = '1102'   --中小板
                AND IS_VALID = 1) T1
    ON (T.JYS = T1.JYS AND T.ZQLB = T1.ZQLB)
  LEFT JOIN (SELECT PARAM_VALUE AS ZQPZ, PARAM_VALUE2 AS ZQDM
               FROM EMC_PUB.T_PARAM_VALUE
              WHERE PARAM_KIND = '01') T2   -- 这个不变
    ON T.ZQDM = T2.ZQDM;

-----STEP3：生成目标表数据
INSERT OVERWRITE TABLE DSC_BAS.T_JGMXLS_HIS
PARTITION
    (CJRQ = ${hivevar:RQ})
    (SEQNO,
     LSH,
     JGZLLB,
     WTH,
     KHH,
     KHXM,
     GDH,
     GDXM,
     JYS,
     JSLX,
     JSJG,
     JSZH,
     BZ,
     WTFS,
     YYB,
     XWDM,
     ZQDM,
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
     BZS1,
     S1,
     S2,
     S3,
     S4,
     S5,
     S6,
     YSJE,
     S11,
     S12,
     S13,
     S15,
     S16,
     YSJE_YJ,
     BCYE,
     ZJMX_LSH,
     DJJE,
     YSSL,
     BCSL,
     BCDJSL,
     DJSL,
     BCYK,
     SETTLE_DATE,
     RZRQ,
     FSYYB,
     LOGINID,
     FHGY,
     JYJ,
     YYS,
     ZQPZ)
    SELECT DISTINCT SEQNO,
           LSH,
           JGZLLB,
           WTH,
           KHH,
           KHXM,
           GDH,
           GDXM,
           JYS,
           JSLX,
           JSJG,
           JSZH,
           BZ,
           WTFS,
           YYB,
           XWDM,
           ZQDM,
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
           ABS(NVL(CJJE,0)) AS CJJE,
           LXJE,
           BZS1,
           S1,
           S2,
           S3,
           S4,
           S5,
           S6,
           NVL(YSJE,0) AS YSJE,
           S11,
           S12,
           S13,
           S15,
           S16,
           YSJE_YJ,
           BCYE,
           ZJMX_LSH,
           DJJE,
           YSSL,
           BCSL,
           BCDJSL,
           DJSL,
           BCYK,
           SETTLE_DATE,
           RZRQ,
           FSYYB,
           LOGINID,
           FHGY,
           JYJ,
           YYS,
           ZQPZ
      FROM TEMP.TEMP_T_JGMXLS_HIS_03 T;

-----STEP99:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_01;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_02;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_03;
DROP TABLE IF EXISTS TEMP.TEMP_T_JGMXLS_HIS_01_1;
DROP TABLE IF EXISTS TEMP.TEMP_JGMXLS_QYDM;

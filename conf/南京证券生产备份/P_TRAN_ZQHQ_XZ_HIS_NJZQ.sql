/*
    @@name:部分证券行情修正
    @@componet_type: Inceptor
    @@author:燕居庆
    @@version: V1.0.1
    @@update_time:2019-11-18
    @@comment:EMC_APEX 该脚本针对南京证券部分证券代码行情时有时无问题进行修正，写死证券代码不管有无都写死
    @@注意事项：1.只适合南京证券
    修改记录
    ------------------------------------------------
    燕居庆  20191118  创建
	燕居庆  20200109  去除证券代码为000000的行情
*/

-----初始化参数变量-----
set character.literal.as.string=true;

-----STEP 0:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_1;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_2;

-----STEP 1：创建临时表
CREATE TABLE TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_1
AS
SELECT JYS,
     ZQDM,
     ZQMC,
     JYDW,
     (case when ZQDM='518850' and rq = 20200604 then
     3.8807
     else
     ZXJ end) as ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     LXJG,
     JJJYBZ,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG
    FROM DSC_BAS.T_ZQHQ_XZ_HIS WHERE RQ = ${hivevar:RQ} 
    AND ZQDM NOT IN ('751026', '751005', '751007', '751032', '000000'); --不管有没有全部剔除


CREATE TABLE TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_2
AS
SELECT DISTINCT JYS, --防止当天行情重复
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     LXJG,
     JJJYBZ,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG FROM (
    SELECT * FROM dsc_bas.t_zqhq_xz_his 
    WHERE rq > 20191001 
    AND zqdm IN ('751026', '751005', '751007', '751032') 
    ORDER BY rq desc,zqdm LIMIT 4) T; --直接选取最近日期的数据


-----STEP 8:生成目标表
INSERT OVERWRITE TABLE DSC_BAS.T_ZQHQ_XZ_HIS
PARTITION
    (RQ = ${hivevar:RQ})
    (JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     LXJG,
     JJJYBZ,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG) 
     SELECT JYS,
     ZQDM,
     ZQMC,
     JYDW,
     ZXJ,
     ZSP,
     JKP,
     ZGJ,
     ZDJ,
     CJSL,
     CJJE,
     ZXLX,
     LXJG,
     JJJYBZ,
     ZXJ_ORG,
     ZSP_ORG,
     GZJ_FLAG
    FROM (
    SELECT JYS,
         ZQDM,
         ZQMC,
         JYDW,
         ZXJ,
         ZSP,
         JKP,
         ZGJ,
         ZDJ,
         CJSL,
         CJJE,
         ZXLX,
         LXJG,
         JJJYBZ,
         ZXJ_ORG,
         ZSP_ORG,
         GZJ_FLAG FROM TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_1
    UNION ALL
     SELECT JYS,
         ZQDM,
         ZQMC,
         JYDW,
         ZXJ,
         ZSP,
         JKP,
         ZGJ,
         ZDJ,
         CJSL,
         CJJE,
         ZXLX,
         LXJG,
         JJJYBZ,
         ZXJ_ORG,
         ZSP_ORG,
         GZJ_FLAG FROM TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_2) T;


-----STEP 99:删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_2;
DROP TABLE IF EXISTS TEMP.TEMP_T_ZQHQ_XZ_HIS_NJZQ_1;
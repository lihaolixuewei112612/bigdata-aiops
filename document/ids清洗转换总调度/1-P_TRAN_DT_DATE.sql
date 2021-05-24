 /* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能:生成指定统计日期的数据(只保留最近执行的交易日的数据)                                 */
 /* 脚本归属:USR                                                                                  */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员    操作日期    操作说明                                                     */
 /*          李伟        20180705    创建                                                         */
 /*          燕居庆	     20181206    修改脚本适应Inceptor语句
 /* ********************************************************************************************* */

--STEP 0: 删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_DT_DATE;

--STEP 1: 创建临时表
CREATE TABLE TEMP.TEMP_T_DT_DATE
AS
SELECT * FROM PUB_SYS.T_DT_DATE
 WHERE 1 = 2;


--STEP 2: 将当天的统计数据插入临时表
INSERT OVERWRITE TABLE temp.TEMP_T_DT_DATE
    SELECT 1, 1, ${hivevar:RQ}, SYSTIMESTAMP FROM system.dual;

--STEP 3: 将临时表中的数据插入目标表
INSERT OVERWRITE TABLE PUB_SYS.T_DT_DATE
    (TJLX, TJLD, TJRQ, CREATE_TIME)
    SELECT TJLX, TJLD, TJRQ, CREATE_TIME
    FROM TEMP.TEMP_T_DT_DATE;
--STEP 4：刷新汇率udf缓存
select  f_invalidate_udf_cache('f_get_hlcs') from system.dual;

--STEP 99: 删除临时表
DROP TABLE IF EXISTS TEMP.TEMP_T_DT_DATE;

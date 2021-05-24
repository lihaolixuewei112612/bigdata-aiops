 /* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能:行情_金融产品行情转换_多金                                                           */
 /* 脚本归属:EMC_APFP                                                             				  */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员		操作日期		操作说明                                              */
 /*          李伟		    20180821		创建                                                  */
 /*          晏苏伟		    20190802		过滤APP_ID为5的基金净值，保留APP_ID为2的基金净值      */
 /* ********************************************************************************************* */

--STEP 0：删除临时表
DROP TABLE IF EXISTS TEMP.TMP_T_JRCPHQ_HIS_BAK;
DROP TABLE IF EXISTS TEMP.TMP_T_JRCPHQ_HIS_01;

--STEP 1 :创建临时表用于存储开放式基金的行情数据
CREATE TABLE TEMP.TMP_T_JRCPHQ_HIS_BAK
AS
SELECT * FROM DSC_BAS.T_JRCPHQ_HIS
WHERE APP_ID != 5
AND RQ = ${hivevar:RQ};

--STEP 2：创建临时表
CREATE TABLE TEMP.TMP_T_JRCPHQ_HIS_01
AS
SELECT * FROM DSC_BAS.T_JRCPHQ_HIS
WHERE 1 = 2;

--STEP 3:将源表[SRC_APEXFP.TFP_CPJZ]的数据清洗转换到临时表[TEMP.TMP_T_JRCPHQ_HIS_01]中
INSERT INTO TEMP.TMP_T_JRCPHQ_HIS_01
    (RQ, CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID)
    SELECT ${hivevar:RQ} AS RQ,
           F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_JRCPHQ_HIS_DJ', 'CPFL', 1, 1, B.CPFL) AS CPFL,
           A.CPDM AS CPDM,
           A.JGDM AS FXJG,
           CAST('1' AS STRING)  BZ,
           A.LJJZ,
           A.CPJZ AS ZXJZ,
           A.JGDM AS JRJGDM,
           5 AS APP_ID
      FROM SRC_APEXFP.TFP_CPJZ A, SRC_APEXFP.TFP_CPDM B
     WHERE A.CPID = B.ID
       AND A.RQ = ${hivevar:RQ};

--STEP 4:将数据插入目标表
INSERT OVERWRITE TABLE DSC_BAS.T_JRCPHQ_HIS
    PARTITION
    (RQ = ${hivevar:RQ})(CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID)
    SELECT CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID
      FROM (SELECT RQ, CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID
              FROM TEMP.TMP_T_JRCPHQ_HIS_BAK
            UNION ALL
            SELECT RQ, CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID
              FROM TEMP.TMP_T_JRCPHQ_HIS_01 A
			 WHERE NOT EXISTS (SELECT 1
                      FROM TEMP.TMP_T_JRCPHQ_HIS_BAK B   --过滤APP_ID为5的基金净值，保留APP_ID为2的基金净值（5：OTC系统，2：集中柜台系统）
                     WHERE B.APP_ID != 5
                       AND A.CPDM = B.CPDM)) T;

--STEP 99：删除临时表
DROP TABLE IF EXISTS TEMP.TMP_T_JRCPHQ_HIS_BAK;
DROP TABLE IF EXISTS TEMP.TMP_T_JRCPHQ_HIS_01;

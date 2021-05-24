/*
	@@name:汇率参数扩展表清洗转换过程
	@@componet_type: inceptor
	@@author:李伟
	@@version: V1.0.0
    @@update_time:2019-04-16
	@@comment:EMC_APEX 修改：对SRC_ABOSS.THLCS表的BZ2 = 'RMB'进行限制

    燕居庆   20190826  港币汇率调整为从dsc_cfg.t_hlcs表进行获取
 */

    --STEP 0：删除临时表
	DROP TABLE IF EXISTS TEMP.TMP_T_HLCS_EXT_BAK;
	DROP TABLE IF EXISTS TEMP.TMP_T_HLCS_EXT_01;

	--STEP 1：备份历史数据
	CREATE TABLE TEMP.TMP_T_HLCS_EXT_BAK
	AS
	SELECT * FROM DSC_CFG.T_HLCS_EXT
	WHERE RQ <> ${hivevar:RQ};

	--1.1 创建临时表
	CREATE TABLE TEMP.TMP_T_HLCS_EXT_01
	AS
	SELECT * FROM DSC_CFG.T_HLCS_EXT
	WHERE 1 = 2;


	--STEP 2：将源表[SRC_ABOSS.THLCS]和[SRC_ABOSS.TJGMXLS]的汇率参数数据清洗转换到目标表[TEMP.TMP_T_HLCS_EXT_01]中
    INSERT INTO TEMP.TMP_T_HLCS_EXT_01
            (RQ, BZ, JSBZ, XCMRJ, XCMCJ, XHMRJ, XHMCJ, XGRQ, SJMRJ, SJMCJ)
            SELECT ${hivevar:RQ} AS RQ,
                   A.BZ,
                   1 as JSBZ,
                   0 as XCMRJ,
                   0 as XCMCJ,
                   NVL(A.GSBL, 0.8) AS XHMRJ,  --港币汇率调整为从dsc_cfg.t_hlcs表进行获取
                   0 as XHMCJ,
                   0 as XGRQ,
                   0 as  SJMRJ,
                   0 as SJMCJ
              FROM DSC_CFG.T_HLCS A
                WHERE ${hivevar:RQ} BETWEEN A.KSRQ AND A.JSRQ;

	--STEP 3 ：将数据插入目标表
	INSERT OVERWRITE TABLE DSC_CFG.T_HLCS_EXT
		(RQ, BZ, JSBZ, XCMRJ, XCMCJ, XHMRJ, XHMCJ, XGRQ, SJMRJ, SJMCJ)
		SELECT RQ, BZ, JSBZ, XCMRJ, XCMCJ, XHMRJ, XHMCJ, XGRQ, SJMRJ, SJMCJ
		  FROM TEMP.TMP_T_HLCS_EXT_01
		UNION ALL
		SELECT RQ, BZ, JSBZ, XCMRJ, XCMCJ, XHMRJ, XHMCJ, XGRQ, SJMRJ, SJMCJ
		  FROM TEMP.TMP_T_HLCS_EXT_BAK;

	--STEP 99：删除临时表
	DROP TABLE IF EXISTS TEMP.TMP_T_HLCS_EXT_BAK;
	DROP TABLE IF EXISTS TEMP.TMP_T_HLCS_EXT_01;

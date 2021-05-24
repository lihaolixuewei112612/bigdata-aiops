 /* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能: BSS_机构管理（营业部）清洗转换过程                                                  */
 /* 脚本归属:EMC_BSS                                                                              */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员    操作日期    操作说明                                                     */
 /*          李伟        20180504    创建                                                         */
 /*          燕居庆      20190403    修改适应Inceptor语法                                         */
 /* ********************************************************************************************* */

    --将源表[SRC_BSS.LBORGANIZATION]的数据清洗转换到目标表[DSC_CFG.T_JGGL]中
	 INSERT OVERWRITE TABLE DSC_CFG.T_JGGL
		(ORGCODE,
		 FDNCODE,
		 `TYPE`,
		 DZ,
		 PYDM,
		 CREATE_DAY,
		 DQFL,
		 YYBFL,
		 SX,
		 ZXDH,
		 JNSEQ,
		 PROVINCE,
		 GLDQ,
		 `DESCRIBE`,
		 CITY,
		 ID,
		 NAME,
		 GRADE,
		 ORGTYPE,
		 NOTE,
		 FID)
		SELECT A.ORGCODE,
			   A.FDNCODE,
			   A.`TYPE`,
			   A.DZ,
			   A.PYDM,
			   CAST(A.RQ AS INT) AS RQ,
			   CAST(A.DQFL AS STRING) AS DQFL,
			   CAST(A.YYBFL AS STRING) AS YYBFL,
			   A.SX,
			   A.ZXDH,
			   0 AS JNSEQ, --A.JNSEQ,
			   CAST(A.PROVINCE AS STRING) AS PROVINCE,
			   CAST(A.GLDQ AS BIGINT) AS GLDQ,
			   A.`DESCRIBE`,
			   CAST(A.CITY AS STRING) AS CITY,
			   A.ID,
			   A.NAME,
			   A.GRADE,
			   A.ORGTYPE,
			   A.NOTE,
			   A.FID
		  FROM SRC_BSS.LBORGANIZATION A;

/* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能:业务科目表清洗转换过程                                                               */
 /* 脚本归属:EMC_APEX                                                                             */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员    操作日期    操作说明                                                     */
 /*          李伟        20171225    创建                                                         */
 /*          燕居庆         20181210    修改脚本适应Inceptor语法                                   */
 /* ********************************************************************************************* */
    set character.literal.as.string=true;
        --STEP0：将源表[SRC_ABOSS.TYWKM和SRC_ABOSS.TXY_YWKM]的数据清洗转换到目标表[DSC_CFG.T_YWKM]中
        INSERT OVERWRITE TABLE DSC_CFG.T_YWKM
            (YWKM, YWKMMC, YWKMLB, SJKM)
            SELECT A.YWKM, A.YWKMMC, NVL(B.PARAM_KIND, '99') AS YWKMLB, A.SJKM
              FROM SRC_ABOSS.TYWKM A
              LEFT JOIN (SELECT *
                           FROM DSC_STAT.T_PARAM_VALUE
                          WHERE PARAM_KIND IN ('01', '02')) B
                ON A.YWKM = B.PARAM_VALUE
            UNION ALL
            SELECT A.YWKM, A.YWKMMC, NVL(B.PARAM_KIND, '99') AS YWKMLB, A.SJKM
              FROM SRC_ABOSS.TXY_YWKM A
              LEFT JOIN (SELECT *
                           FROM DSC_STAT.T_PARAM_VALUE
                          WHERE PARAM_KIND IN ('01', '02')) B
                ON A.YWKM = B.PARAM_VALUE
             WHERE NOT EXISTS (SELECT 1 FROM SRC_ABOSS.TYWKM T WHERE A.YWKM = T.YWKM)   --排除集中的业务科目
            UNION ALL
            SELECT A.YWKM, A.YWKMMC, NVL(B.PARAM_KIND, '99') AS YWKMLB, A.SJKM
              FROM SRC_ABOSS.TSO_YWKM A
              LEFT JOIN (SELECT *
                           FROM DSC_STAT.T_PARAM_VALUE
                          WHERE PARAM_KIND IN ('01', '02')) B
                ON A.YWKM = B.PARAM_VALUE
             WHERE NOT EXISTS (SELECT 1 FROM SRC_ABOSS.TYWKM T1 WHERE A.YWKM = T1.YWKM) --排除集中的业务科目
			 AND NOT EXISTS (SELECT 1 FROM SRC_ABOSS.TXY_YWKM T2 WHERE A.YWKM = T2.YWKM); --排除信用的业务科目

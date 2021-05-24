/* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能:行情_金融产品行情清洗转换过程                                                        */
 /* 脚本归属:EMC_APEX                                                                             */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员    操作日期    操作说明                                                     */
 /*          李伟        20171225    创建                                                         */
 /*          燕居庆         20181210    修改脚本适应Inceptor语法                                     */
 /* ********************************************************************************************* */
    set character.literal.as.string=true;
    --STEP0：将源表[SRC_ABOSS.TOF_JJXX]的数据清洗转换到目标表[DSC_BAS.T_JRCPHQ_HIS]APP_ID=2中
    INSERT OVERWRITE TABLE DSC_BAS.T_JRCPHQ_HIS
        PARTITION (RQ = ${hivevar:RQ})
        (CPFL, CPDM, FXJG, BZ, LJJZ, ZXJZ, JRJGDM, APP_ID)
        SELECT '1' AS CPFL, --??产品分类的函数还未开发，暂时配置为1:开放式基金
               A.JJDM AS CPDM,
               A.TADM AS FXJG,
               F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_JRCPHQ_HIS', 'BZ', 1, 1, A.BZ) AS BZ,
               A.LJJZ AS LJJZ,
                CAST(ROUND(A.JJJZ,4) AS DECIMAL(10,4)) AS ZXJZ,
               A.TADM AS JRJGDM,
               2 AS APP_ID
          FROM SRC_ABOSS.TOF_JJXX A;

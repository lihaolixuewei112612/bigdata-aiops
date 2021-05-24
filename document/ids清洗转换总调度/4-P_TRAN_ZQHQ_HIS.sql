 /* ***************************************** SQL Begin ***************************************** */
 /* 脚本功能:证券行情表清洗转换程序                                                             */
 /* 脚本归属:EMC_APEX                                                                               */
 /* 修改记录:                                                                                     */
 /* ----------------------------------------------------------------------------------------------*/
 /*          操作人员        操作日期        操作说明                                              */
 /*          元健            20171222        创建                                                  */
 /*          燕居庆            20181206        修改脚本适应Inceptor语法                              */
 /* ********************************************************************************************* */
 set character.literal.as.string=true;
INSERT OVERWRITE TABLE DSC_BAS.T_ZQHQ_HIS
    PARTITION (RQ = ${hivevar:RQ})
    (JYS, ZQDM, JYDW, ZXJ, ZSP, JKP, ZGJ, ZDJ, CJSL, CJJE, ZXLX)
    SELECT F_GET_ETL_TRAN_DICVAL('DSC_BAS', 'T_ZQHQ_HIS', 'JYS', 1, 1, A.JYS) AS JYS,
           ZQDM,
           JYDW,
           ZXJ,
           ZSP,
           JKP,
           ZGJ,
           ZDJ,
           CJSL,
           CJJE,
           ZXLX
      FROM SRC_ABOSS.TZQHQ A
     WHERE A.RQ = ${hivevar:RQ};

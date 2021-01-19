package com.apex_ids.util.MyEnum;

/**
 * Created by caigx
 * Date: 2019/8/7
 * Time: 16:43
 * Description: No Description
 */
public enum CustCalsylInStandard {
    khh("string"),
    zzc("decimal(16,2)"),
    zcjlr("decimal(16,2)"),
    zfz("decimal(16,2)"),
    yk("decimal(16,2)"),
    rq("int"),
    zjye("decimal(16,2)"),

    crje("decimal(16,2)"),
    qcje("decimal(16,2)"),
    zrzqsz("decimal(16,2)"),
    zczqsz("decimal(16,2)"),
    qtsr("decimal(16,2)");
    /*CRJE("decimal(16,2)"),
    ZRZQSZ("decimal(16,2)"),
    QTSR("decimal(16,2)"),
    QCJE("decimal(16,2)"),
    ZCZQSZ("decimal(16,2)"),
    QTZC("decimal(16,2)");*/

    private String type;
    CustCalsylInStandard(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }


}

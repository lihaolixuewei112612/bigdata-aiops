package com.apex_ids.util.MyEnum;

public enum DailyBillConf {
    khh("string"),
    rq("int"),
    zzc("decimal(16,2)"),
    zzc_jzjy("decimal(16,2)"),
    zzc_rzrq("decimal(16,2)"),
    zzc_ggqq("decimal(16,2)"),
    zjye("decimal(16,2)"),
    zjye_jzjy("decimal(16,2)"),
    zjye_rzrq("decimal(16,2)"),
    zjye_ggqq("decimal(16,2)"),
    zcjlr("decimal(16,2)"),
    crje("decimal(16,2)"),
    qcje("decimal(16,2)"),
    zrzqsz("decimal(16,2)"),
    zczqsz("decimal(16,2)"),
    qtsr("decimal(16,2)"),
    yk("decimal(16,2)"),
    yk_jzjy("decimal(16,2)"),
    yk_rzrq("decimal(16,2)"),
    yk_jrcp("decimal(16,2)"),
    yk_ggqq("decimal(16,2)"),
    zqsz("decimal(16,2)"),
    zqsz_jzjy("decimal(16,2)"),
    zqsz_rzrq("decimal(16,2)"),
    zqsz_jrcp("decimal(16,2)"),
    zqsz_ggqq("decimal(16,2)"),
    zfz("decimal(16,2)"),
    zfz_rzrq("decimal(16,2)"),
    zxjz("decimal(9,6)"),
    zxjz_jzjy("decimal(9,6)"),
    zxjz_rzrq("decimal(9,6)"),
    zxjz_ggqq("decimal(9,6)");

    private String type;
    DailyBillConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

package com.apex_ids.util.MyEnum;

public enum CostConf {
    lsh("long"),
    tzzh_id("int"),
    seqno("long"),
    wth("int"),
    khh("string"),
    khxm("string"),
    gdh("string"),
    jys("string"),
    bz("string"),
    yyb("string"),
    zqdm("string"),
    zqmc("string"),
    zqlb("string"),
    jylb("string"),
    cjbh("string"),
    cjrq("int"),
    cjsj("string"),
    sbsj("string"),
    cjbs("int"),
    cjsl("decimal(16,2)"),
    cjjg("decimal(9,4)"),
    jsj("decimal(16,8)"),
    lxjg("decimal(16,8)"),
    cjje("decimal(16,2)"),
    lxje("decimal(16,2)"),
    ysje("decimal(16,2)"),
    jsrq("int"),
    bczqye("decimal(16,2)"),
    yssl("decimal(16,2)"),
    seqno_qs("long"),
    bczqye_qs("decimal(16,2)"),
    cjsj_qs("string"),
    cccb_qs("decimal(16,2)"),
    cbj_qs("decimal(9,4)"),
    sxyk_qs("decimal(16,2)"),
    ljyk_qs("decimal(16,2)"),
    jylb_qs("string"),
    zt_qs("int"),
    rownum("int"),
    s1("decimal(16,2)"),
    s2("decimal(16,2)"),
    s3("decimal(16,2)"),
    s4("decimal(16,2)"),
    s5("decimal(16,2)"),
    s6("decimal(16,2)"),
    zxj("decimal(9,4)"),
    jjjz("decimal(14,4)"),
    remark_qs("string"),
    cccb_zc("decimal(16,2)"),
    cccb_zr("decimal(16,2)"),
    yk_zc("decimal(16,2)"),
    yk_zr("decimal(16,2)"),
    jyfy("decimal(16,2)"),
    zqsl_cc("int"),
    cccb_cc("decimal(16,2)"),
    ljyk_cc("decimal(16,2)"),
    cbj_cc("decimal(9,4)"),
    fj_khh("string");

    private String type;
    CostConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

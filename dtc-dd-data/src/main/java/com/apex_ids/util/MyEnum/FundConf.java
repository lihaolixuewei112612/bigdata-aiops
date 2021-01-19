package com.apex_ids.util.MyEnum;

public enum FundConf {
    tzzh_id("int"),
    khh("string"),
    gdh("string"),
    seqno_qs("long"),
    jylb_qs("string"),
    cjrq("int"),
    cjbh("string"),
    jys("string"),
    zqdm_fjmj("string"),
    flag("int"),
    mjdm("string"),
    mjdm_cjjg("decimal(9,4)"),
    mjdm_yssl("decimal(16,2)"),
    mjdm_ysje("decimal(16,2)"),
    mjdm_bcye("decimal(16,2)"),
    mjdm_cccb("decimal(16,2)"),
    mjdm_cbj("decimal(16,7)"),
    mjdm_dcyk("decimal(16,2)"),
    mjdm_ljyk("decimal(16,2)"),
    ajdm("string"),
    ajdm_cjjg("decimal(9,4)"),
    ajdm_yssl("decimal(16,2)"),
    ajdm_ysje("decimal(16,2)"),
    ajdm_bcye("decimal(16,2)"),
    ajdm_cccb("decimal(16,2)"),
    ajdm_cbj("decimal(16,7)"),
    ajdm_dcyk("decimal(16,2)"),
    ajdm_ljyk("decimal(16,2)"),
    bjdm("string"),
    bjdm_cjjg("decimal(9,4)"),
    bjdm_yssl("decimal(16,2)"),
    bjdm_ysje("decimal(16,2)"),
    bjdm_bcye("decimal(16,2)"),
    bjdm_cccb("decimal(16,2)"),
    bjdm_cbj("decimal(16,7)"),
    bjdm_dcyk("decimal(16,2)"),
    bjdm_ljyk("decimal(16,2)"),
    cccb_zy("decimal(16,2)"),
    ljyk_zy("decimal(16,2)"),
    cccb_zy_ahm("decimal(16,2)"),
    ljyk_zy_ahm("decimal(16,2)"),
    cccb_zy_bhm("decimal(16,2)"),
    ljyk_zy_bhm("decimal(16,2)"),
    jyfy("decimal(16,2)"),
    remark("string"),
    rn("int");
    private String type;
    FundConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

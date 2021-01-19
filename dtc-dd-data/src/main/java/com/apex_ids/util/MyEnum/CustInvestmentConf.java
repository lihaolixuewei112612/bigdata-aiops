package com.apex_ids.util.MyEnum;

public enum  CustInvestmentConf {
    khh("string"),
    bdl("decimal(16,2)"),
    sharp("decimal(16,2)"),
    zdhcl("decimal(16,2)"),
    beta("decimal(16,2)"),
    calma("decimal(16,2)"),
    syl("decimal(16,2)"),
    hchf("string"),
    pmnl_pf("decimal(16,2)"),
    pmnl_pj("string"),
    pmnl("string"),
    pmnl_zs("string"),
    ylnl_pf("decimal(16,2)"),
    ylnl_pj("string"),
    dpsyl("decimal(16,2)"),
    syl_db_dp("decimal(16,2)"),
    fknl_pf("decimal(16,2)"),
    fknl_pj("string"),
    bdl_bq("string"),
    hc_bq("string");
    private String type;
    CustInvestmentConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

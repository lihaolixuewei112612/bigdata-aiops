package com.apex_ids.util.MyEnum;

public enum YearBillConfHengtaiIn {
    khh("string"),
    rq("int"),
    zzc("decimal(16,2)"),
    zcjlr("decimal(16,2)"),
    yk("decimal(16,2)"),
    zfz("decimal(16,2)"),
    zxjz("decimal(9,6)"),
    zjye("decimal(16,2)");
    private String type;
    YearBillConfHengtaiIn(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

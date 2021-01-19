package com.apex_ids.util.MyEnum;

public enum YearBillCwkzIn {
    khh("string"),
    rq("int"),
    zqsz_jzjy("decimal(16,2)"),
    zqsz_rzrq("decimal(16,2)"),
    ;

    private String type;
    YearBillCwkzIn(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

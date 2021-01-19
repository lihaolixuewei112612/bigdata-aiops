package com.apex_ids.util.MyEnum;

public enum AnalyCwkzIn {
    khh("string"),
    rq("int"),
    zqsz_jzjy("decimal(16,2)"),
    zqsz_rzrq("decimal(16,2)"),
    zshq_hs300("decimal(16,2)"),
    ;

    private String type;
    AnalyCwkzIn(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

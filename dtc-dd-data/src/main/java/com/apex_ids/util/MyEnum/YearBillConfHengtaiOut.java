package com.apex_ids.util.MyEnum;

public enum YearBillConfHengtaiOut {
    khh("string"),
    //year("int"),
    zbz("object"),
    order_id("float"),
    zbmc("string")
    ;

    private String type;
    YearBillConfHengtaiOut(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

package com.apex_ids.util.MyEnum;

/**
 * Created by caigx
 * Date: 2019/8/7
 * Time: 16:43
 * Description: No Description
 */
public enum CustCalsylOut {
    khh("string"),
    zzcJoin("string");

    private String type;
    CustCalsylOut(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }


}

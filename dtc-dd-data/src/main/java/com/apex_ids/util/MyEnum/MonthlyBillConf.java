package com.apex_ids.util.MyEnum;

import java.util.Arrays;
import java.util.List;

public enum MonthlyBillConf {
    khh("string"),
    yf("int"),
    qczzc("decimal(16,2)"),
    qczfz("decimal(16,2)"),
    qczzc_jzjy("decimal(16,2)"),
    qczzc_rzrq("decimal(16,2)"),
    qczzc_ggqq("decimal(16,2)"),
    yk_jzjy("decimal(16,2)"),
    yk_rzrq("decimal(16,2)"),
    yk_jrcp("decimal(16,2)"),
    yk_ggqq("decimal(16,2)"),
    qmzzc("decimal(16,2)"),
    qmzjye("decimal(16,2)"),
    qmzqsz("decimal(16,2)"),
    qmzfz("decimal(16,2)"),
    qmzzc_jzjy("decimal(16,2)"),
    qmzqsz_jzjy("decimal(16,2)"),
    qmzjye_jzjy("decimal(16,2)"),
    qmzxjz_jzjy("decimal(9,6)"),
    qmzzc_rzrq("decimal(16,2)"),
    qmzqsz_rzrq("decimal(16,2)"),
    qmzjye_rzrq("decimal(16,2)"),
    qmzfz_rzrq("decimal(16,2)"),
    qmzxjz_rzrq("decimal(9,6)"),
    qmzqsz_jrcp("decimal(16,2)"),
    qmzzc_ggqq("decimal(16,2)"),
    qmzjye_ggqq("decimal(16,2)"),
    qmzqsz_ggqq("decimal(16,2)"),
    qmzxjz_ggqq("decimal(9,6)"),
    byts("int"),
    zcjlr("decimal(16,2)"),
    zxjz_zzl("decimal(9,6)"),
    zxjz_zzl_jzjy("decimal(9,6)"),
    zxjz_zzl_rzrq("decimal(9,6)"),
    zxjz_zzl_ggqq("decimal(9,6)"),
    yk("decimal(16,2)"),
    crje("decimal(16,2)"),
    qcje("decimal(16,2)"),
    zrzqsz("decimal(16,2)"),
    zczqsz("decimal(16,2)"),
    ykl("decimal(9,6)"),
    bdl("decimal(9,6)"),
    zdhcl("decimal(9,6)"),
    nhsyl("decimal(9,6)"),
    pjzzc("decimal(16,2)"),
    pjsz("decimal(16,2)");
    private String type;
    MonthlyBillConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }

}

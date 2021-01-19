package com.apex_ids.util.MyEnum;

public enum CustDailyBillConf{
	// bill

	Khh("string"),
	Zzc("decimal(16,2)"),
	Zzc_rzrq("decimal(16,2)"),
	Yk("decimal(16,2)"),  		// (sum)
	Crje("decimal(16,2)"),
	Qcje("decimal(16,2)"),
	Zrzqsz("decimal(16,2)"),
	Zczqsz("decimal(16,2)"),
	Yk_jzjy("decimal(16,2)"),
	Yk_rzrq("decimal(16,2)"),
	Yk_jrcp("decimal(16,2)"),
	Yk_ggqq("decimal(16,2)"),
	Zqsz("decimal(16,2)"),
	Zcjlr("decimal(16,2)"),
	Zjye("decimal(16,2)"),
	Zfz("decimal(16,2)"),
	Zzc_jzjy("decimal(16,2)"),
	Zqsz_jzjy("decimal(16,2)"),
	Zjye_jzjy("decimal(16,2)"),
	Zxjz_jzjy("decimal(16,2)"),
	Zqsz_rzrq("decimal(16,2)"),
	Zjye_rzrq("decimal(16,2)"),
	Zfz_rzrq("decimal(16,2)"),
	Zxjz_rzrq("decimal(9,6)"),
	Zqsz_jrcp("decimal(16,2)"),
	Zzc_ggqq("decimal(16,2)"),
	Zjye_ggqq("decimal(16,2)"),
	Zqsz_ggqq("decimal(16,2)"),
	Zxjz_ggqq("decimal(9,6)"),
	Zxjz("decimal(16,2)"),
	// monthlyBill
	//Khh("string"),
	Qczzc("decimal(16,2)"),
	Qczzc_jzjy("decimal(16,2)"),
	Qczzc_rzrq("decimal(16,2)"),
	Qczzc_ggqq("decimal(16,2)"),
	//Yk("decimal(16,2)"),
	//Crje("decimal(16,2)"),
	//Qcje("decimal(16,2)"),
	//Zrzqsz("decimal(16,2)"),
	//Zczqsz("decimal(16,2)"),
	//Yk_jzjy("decimal(16,2)"),
	//Yk_rzrq("decimal(16,2)"),
	//Yk_jrcp("decimal(16,2)"),
	//Yk_ggqq("decimal(16,2)"),
	Qmzzc("decimal(16,2)"),
	Qmzjye("decimal(16,2)"),
	Qmzqsz("decimal(16,2)"),
	Qmzfz("decimal(16,2)"),
	Qmzzc_jzjy("decimal(16,2)"),
	Qmzqsz_jzjy("decimal(16,2)"),
	Qmzjye_jzjy("decimal(16,2)"),
	Qmzxjz_jzjy("decimal(9,6)"),
	Qmzzc_rzrq("decimal(16,2)"),
	Qmzqsz_rzrq("decimal(16,2)"),
	Qmzjye_rzrq("decimal(16,2)"),
	Qmzfz_rzrq("decimal(16,2)"),
	Qmzxjz_rzrq("decimal(9,6)"),
	Qmzqsz_jrcp("decimal(16,2)"),
	Qmzzc_ggqq("decimal(16,2)"),
	Qmzjye_ggqq("decimal(16,2)"),
	Qmzqsz_ggqq("decimal(16,2)"),
	Qmzxjz_ggqq("decimal(9,6)"),
	Byts("int"),
	//Zcjlr("decimal(16,2)"),
	Zxjz_zzl("decimal(9,6)"),
	Zxjz_zzl_jzjy("decimal(9,6)"),
	Zxjz_zzl_rzrq("decimal(9,6)"),
	Zxjz_zzl_ggqq("decimal(9,6)"),
	Ykl("decimal(9,6)"),
	Bdl("decimal(9,6)"),
	Zdhcl("decimal(9,6)"),
	Nhsyl("decimal(9,6)"),
	Pjzzc("decimal(16,2)"),
	Pjsz("decimal(16,2)"),
	Rq("int");

	private String type;
	CustDailyBillConf(String type){
        this.type = type;
    }
    public String getType(){
        return this.type;
    }
    public int getIndex(){
        return this.ordinal();
    }
}

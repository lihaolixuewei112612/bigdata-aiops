package com.apex_ids.udtfs;

import com.google.common.collect.Sets;

import java.util.Set;

public class Constants {

	// 买入交易行为（1买入,16送股,61融资买入,41认购,42申购,59大宗买入,60大宗卖出,80申保,83中签）
	// 卖出交易行为（2卖出,6分红,17兑付,22兑息,43赎回,45分红,71融资强平,81退保）

	public static String JYLB_BUY = "1";// 买入
	public static String JYLB_SELL = "2";// 卖出
	public static String JYLB_JK = "3";// 缴款
	public static String JYLB_HL = "6";// 红利
	public static String	JYLB_ZZ="11";//转债
	public static String JYLB_TGZR = "18";// 托管转入
	public static String JYLB_TGZC = "19";// 托管转出
	public static String JYLB_ETFSH = "30";// ETF赎回
	public static String JYLB_XYZR = "31";// 协议转让
	// public static String JYLB_CALL = "41";// 认购
	public static String JYLB_JJSG = "42";// 基金申购
	public static String JYLB_JJSH = "43";// 基金赎回
	public static String JYLB_JJCF = "47";// 基金拆分
	public static String JYLB_JJHB = "48";// 基金合并
	public static String JYLB_HSGK = "81";// 还申购款
	public static String JYLB_CBTZ = "98";// 成本调整

	public static String ZQLB_A = "A0";// A股、普通港股
	public static String ZQLB_ZZ = "Z5";// 转债
	public static String ZQLB_ETF = "E0";// ETF基金
	public static String ZQLB_HBETF = "EH";// 货币ETF
	public static String ZQLB_LOF = "L0";// LOF基金

	public static String JJLX_M = "1";// M基金
	public static String JJLX_A = "A";// A基金
	public static String JJLX_B = "B";// B基金

	public static String CJBH_ZSBG = "折算变更";// 折算变更
	public static String CJBH_SGFQWT = "SGFQWT";// 发起申购委托
	public static String CJBH_SHFQWT = "SHFQWT";// 发起赎回委托
	public static String CJBH_SHJSCG = "SHJSCG";// 赎回到期交收
	public static String CJBH_CFHB = "拆分合并";// 拆分合并
	public static String CJBH_FEBD = "份额变动";
	public static String CJBH_JSSH = "交收顺延";

	public static final String JYS_SH = "2";
	public static Set<String> cjbhList = Sets.newHashSet("SHJSCG", "SHFQWT");
	// 18 托管转入|19 托管转出
	public static Set<String> jylbTGZRZC = Sets.newHashSet("18", "19");
	// 47 基金拆分|48 基金合并
	public static Set<String> jylbHBCF = Sets.newHashSet("47", "48");
	// 41 基金认购| 42基金申购
	public static Set<String> jylbSG = Sets.newHashSet("41", "42");
	// 30 ETF赎回|41 基金认购| 42基金申购|43基金赎回
	public static Set<String> jylbSGSH = Sets.newHashSet("41", "42", "43", "30");
	// 1 买入|18 托管转入|59 买入确认|61 融资买入|80 缴申购款|83 缴中签款 加78
	public static Set<String> jylbMR = Sets.newHashSet("1", "59", "61", "80", "83", "18","93","94","78");
	// 2 卖出|6 红利|17 兑付|19 托管转出|22 国债派息|45 基金分红|60 卖出确认|62 卖券还款|71 融资强平|81 还申购款 加79
	public static Set<String> jylbSY = Sets.newHashSet("2", "6", "17", "22", "45", "60", "62", "71", "81"/*, "19"*/,"76","79");
	// 非主动交易，使用cjje代替ysje进行计算，并按照YSSL的方向进行转出、转入划分
	public static Set<String> jylbSp = Sets.newHashSet("65", "66", "7", "10", "31", "9", "21", "20", "15", "12");


}

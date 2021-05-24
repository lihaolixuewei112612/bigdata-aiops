package com.apex_ids.udfs;

import com.apexsoft.quotaMethod.QuotaFunction;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * @author caigx
 *
 * 获取区间收益率f_ids_get_syl
 */
@Description(
        name = "GetSylByDate",
        value = "_FUNC_(String) - 根据类型返回Double"
)

public class GetSylByDate extends UDF{
    // private static final Logger log = LoggerFactory.getLogger(initProfitRate.class);
    public Double evaluate(String string,int ksrq,int jsrq) throws Exception{
        Properties props = new Properties();
        Double sylMin=0.0d;
        Double sylMax=0.0d;
        try {
            props.load(GetSylByDate.class.getResourceAsStream("/properties/calcSyl.properties"));
            sylMin=Double.parseDouble(props.getProperty("sylMin"));   //-1
            sylMax=Double.parseDouble(props.getProperty("sylMax"));   //10
        } catch (Exception e) {
            e.printStackTrace();
        }
        //26449.67,0.0,0.0,184.8,20200506,23.27;26542.07,0.0,0.0,92.4,20200507,23.27;26542.07,0.0,0.0,0.0,20200508,23.27;27004.07,0.0,0.0,462.0,20200511,23.27;26819.27,0.0,0.0,-184.8,20200512,23.27;26634.47,0.0,0.0,-184.8,20200513,23.27;25987.67,0.0,0.0,-646.8,20200514,23.27;26172.47,0.0,0.0,184.8,20200515,23.27;28759.67,0.0,0.0,2587.2,20200518,23.27;31624.07,0.0,0.0,2864.4,20200519,23.27;34765.67,0.0,0.0,3141.6,20200520,23.27;38276.87,0.0,0.0,3511.2,20200521,23.27;42065.27,0.0,0.0,3788.4,20200522,23.27;39755.27,0.0,0.0,-2310.0,20200525,23.27;41141.27,0.0,0.0,1386.0,20200526,23.27;41880.47,0.0,0.0,739.2,20200527,23.27;39200.87,0.0,0.0,-2679.6,20200528,23.27;39200.87,0.0,0.0,0.0,20200529,23.27;
        String[] dataArray = string.toString().replaceAll("\\s*", "").split(";");
        List<String> dataList=new ArrayList<>();
        for(String s:dataArray){//满足条件时间区间的才加入
            if(Integer.valueOf(s.split(",")[4])>=ksrq && Integer.valueOf(s.split(",")[4])<=jsrq){
                dataList.add(s);
            }
        }
        List<Map<String, Object>> fundList = new ArrayList<>();
        Double result=0.0d;
        List<Object> bill=new ArrayList<>();
        for (int i = 0; i < dataList.size(); ++i) {
            String[] data = ((String) dataList.get(i)).split(",");
           // bill.add(CustCalsylIn.khh.getIndex(),data[0]);
            bill.add(0,data[0]); //zzc
            bill.add(1,data[1]); //zcjlr
            bill.add(2,data[2]); //zfz
            bill.add(3,data[3]);  //yk
            bill.add(4,data[4]);  //rq
            bill.add(5,data[5]);  //zjye
            /*bill.add(6,data[6]);
            bill.add(7,data[7]);
            bill.add(8,data[8]);
            bill.add(9,data[9]);
            bill.add(10,data[10]);*/
            fundList.add(getCustFund(bill));
        }
        try {
            result = QuotaFunction.getSyl(fundList);
            if(result<sylMin){
                result=sylMin;
            }else if(result>sylMax){
                result=sylMax;
            }
            //result = QuotaFunction.getYkl(fundList);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  result;
    }
    private static Map<String, Object> getCustFund(List<Object> bill){
        Map<String, Object> map = new HashMap<>();
        map.put("RQ", bill.get(4));
        map.put("ZZC", bill.get(0));
        map.put("ZCJLR", bill.get(1));
        map.put("ZFZ",bill.get(2));
        map.put("DRYK", bill.get(3));

        map.put("ZJYE",bill.get(5));

        /*map.put("CRJE", bill.get(5));
        map.put("ZRZQSZ", bill.get(6));
        map.put("QTSR", bill.get(7));
        map.put("QCJE", bill.get(8));
        map.put("ZCZQSZ", bill.get(9));
        map.put("QTZC", bill.get(10));*/
        return map;
    }
}

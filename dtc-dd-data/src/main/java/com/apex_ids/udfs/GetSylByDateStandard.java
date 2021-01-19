package com.apex_ids.udfs;

import com.apexsoft.quotaMethod.QuotaFunction;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.*;

/**
 * @author caigx
 */
@Description(
        name = "GetSylByDate",
        value = "_FUNC_(String) - 根据类型返回Double"
)

public class GetSylByDateStandard extends UDF{
    // private static final Logger log = LoggerFactory.getLogger(initProfitRate.class);
    public Double evaluate(String string,int ksrq,int jsrq) throws Exception{
        Properties props = new Properties();
        /*Double sylMin=0.0d;
        Double sylMax=0.0d;
        Double top=0.0d;
        try {
            props.load(GetSylByDateStandard.class.getResourceAsStream("/properties/calcSyl.properties"));
            sylMin=Double.parseDouble(props.getProperty("sylMin"));
            sylMax=Double.parseDouble(props.getProperty("sylMax"));
            //top=Double.parseDouble(props.getProperty("ids.syl.toplimit"));
        } catch (Exception e) {
            e.printStackTrace();
        }*/
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
            bill.add(0,data[0]);
            bill.add(1,data[1]);
            bill.add(2,data[2]);
            bill.add(3,data[3]);
            bill.add(4,data[4]);

            bill.add(5,data[5]);

            bill.add(6,data[6]);
            bill.add(7,data[7]);
            bill.add(8,data[8]);
            bill.add(9,data[9]);
            bill.add(10,data[10]);
            /*bill.add(6,data[6]);
            bill.add(7,data[7]);
            bill.add(8,data[8]);
            bill.add(9,data[9]);
            bill.add(10,data[10]);*/
            fundList.add(getCustFund(bill));
        }
        try {
            result =QuotaFunction.getReturnRateByMultiFunc(fundList);
            //result = QuotaFunction.getSyl(fundList);
            /*if(result<(double)sylMin){
                result=sylMin;
            }else if(result>(double)sylMax){
                result=sylMax;
            }*/
            //result = QuotaFunction.getYkl(fundList);
        }catch (Exception e){
            e.printStackTrace();
        }
        return  result;
    }
    private static Map<String, Object> getCustFund(List<Object> bill){
        Map<String, Object> map = new HashMap<>();

        map.put("ZZC", bill.get(0));
        map.put("ZCJLR", bill.get(1));
        map.put("ZFZ",bill.get(2));
        map.put("DRYK", bill.get(3));
        map.put("RQ", bill.get(4));
        map.put("ZJYE",bill.get(5));

        map.put("CRJE",bill.get(6));
        map.put("QCJE",bill.get(7));
        map.put("ZRZQSZ",bill.get(8));
        map.put("ZCZQSZ",bill.get(9));
        map.put("QTSR",bill.get(10));
        /*map.put("CRJE", bill.get(5));
        map.put("ZRZQSZ", bill.get(6));
        map.put("QTSR", bill.get(7));
        map.put("QCJE", bill.get(8));
        map.put("ZCZQSZ", bill.get(9));
        map.put("QTZC", bill.get(10));*/
        return map;
    }
}

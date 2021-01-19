package com.apex_ids.udfs;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.String;
/**
 * @author ChenTong
 */
@Description(
        name = "GetProfitRate",
        value = "_FUNC_(String) - 根据类型返回Double"
)

public class GetProfitRate extends UDF{
    // private static final Logger log = LoggerFactory.getLogger(initProfitRate.class);
    public DoubleWritable evaluate(Text string) throws Exception{
        String[] dataArray = string.toString().replaceAll("\\s*", "").split(";");
        List<String> dataList = Arrays.asList(dataArray);
        Collections.sort(dataList, new Comparator<String>() {
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        List<Double> zrzcList = new ArrayList();
        List<Double> dayList = new ArrayList();

        for(int i = 0; i < dataList.size(); ++i) {
            String[] data = ((String)dataList.get(i)).split(",");
            if (i == 0) {
                zrzcList.add(Double.parseDouble(data[4].split(":")[1]));
                dayList.add(Double.parseDouble(data[0].split(":")[1]));
            } else if (i == dataList.size() - 1) {
                zrzcList.add(Double.parseDouble(data[3].split(":")[1]));
                dayList.add(Double.parseDouble(data[0].split(":")[1]));
            } else if (Double.parseDouble(data[2].split(":")[1]) - Double.parseDouble(data[1].split(":")[1]) != 0.0D) {
                zrzcList.add(Double.parseDouble(data[2].split(":")[1]) - Double.parseDouble(data[1].split(":")[1]));
                dayList.add(Double.parseDouble(data[0].split(":")[1]));
            }
        }

        double[] zrzcArray = new double[zrzcList.size()];
        double[] dayArray = new double[dayList.size()];

        for(int ix = 0; ix < zrzcList.size(); ++ix) {
            zrzcArray[ix] = (Double)zrzcList.get(ix);
            dayArray[ix] = (Double)dayList.get(ix);
        }

        return new DoubleWritable(GetProfitRate.calcSyl(dayArray, zrzcArray, (int)dayArray[0], (int)dayArray[dayArray.length - 1]));
    }

    private static double calcSyl(double[] days, double[] zrzcs, int beginDate, int endDate) {
        double syl = 0.1D;
        double range = Math.abs(syl);
        double errorRange = 5.0E-6D;
        double lastShift = 0.0D;
        boolean isFind = false;
        days[0] = (double)beginDate;
        int size = days.length;
        days[days.length - 1] = (double)endDate;
        double[] zrzcArray = new double[zrzcs[0] == 0.0D ? size - 1 : size];
        double[] dayArray = new double[zrzcs[0] == 0.0D ? size - 1 : size];
        if (zrzcs[0] == 0.0D) {
            System.arraycopy(zrzcs, 1, zrzcArray, 0, size - 1);
            System.arraycopy(days, 1, dayArray, 0, size - 1);
            zrzcArray[0] = Math.abs(zrzcArray[0]);
        } else {
            zrzcArray = zrzcs;
            dayArray = days;
        }

        if (zrzcArray.length <= 2) {
            return zrzcArray.length == 2 ? GetProfitRate.calcZzl(zrzcArray[0], zrzcArray[1]) : 0.0D;
        } else {
            double startValue = 0.0D - zrzcArray[0];
            double daysCount = (double)GetProfitRate.differentDays((int)dayArray[0], (int)dayArray[dayArray.length - 1]);

            for(int nStep = 1; nStep <= 1000; ++nStep) {
                double shift = startValue;

                for(int i = 1; i <= zrzcArray.length - 1; ++i) {
                    double day = (double)GetProfitRate.differentDays((int)dayArray[0], (int)dayArray[i]);
                    shift += zrzcArray[i] / Math.pow(1.0D + syl, day / daysCount);
                }
                if (shift > errorRange) {
                    if (lastShift < -errorRange) {
                        isFind = true;
                    }
                    if (!isFind) {
                        range *= 2.0D;
                    } else {
                        range /= 2.0D;
                    }

                    lastShift = shift;
                    syl += range;
                } else {
                    if (shift >= -errorRange) {
                        break;
                    }
                    if (lastShift > errorRange) {
                        isFind = true;
                    }
                    if (!isFind) {
                        range *= 2.0D;
                    } else {
                        range /= 2.0D;
                    }

                    lastShift = shift;
                    double temp = syl - range;
                    if (temp < -1.0D) {
                        range -= -1.0D - temp;
                        range /= 2.0D;
                    }
                    syl -= range;
                }
            }

            return syl;
        }
    }

    private static double calcZzl(double qczc, double qmzc) {
        try {
            double syl = qczc == 0.0D ? 0.0D : (qmzc - qczc) / qczc;
            return syl;
        } catch (Exception var6) {
            var6.printStackTrace();
            return 0.0D;
        }
    }

    private static int differentDays(int date1, int date2){
        String str1 = String.valueOf(date1);
        String str2 = String.valueOf(date2);
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        try{
            Date date11 = format.parse(str1);
            Date date22 = format.parse(str2);
            return (int) ((date22.getTime() - date11.getTime()) / (1000*3600*24));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
}

package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.lang.String;
import java.math.BigDecimal;

/**
 * @author ChenTong
 */
@Description(
        name = "GetZdhcl",		   // 最大回撤率
        value = "_FUNC_(String) - 根据类型返回Double"
)

public class GetZdhcl extends UDF{

    // private static final Logger log = LoggerFactory.getLogger(initZdhcl.class);

    public DoubleWritable evaluate(Text string){
        Double zdhcl = 0.0D;
        String[] zxjzArray = string.toString().replaceAll("\\s*", "").split(",");
        List<String> zxjzList = Arrays.asList(zxjzArray);
        Collections.sort(zxjzList, new Comparator<String>() {
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        List<Double> jzList = new ArrayList();
        Iterator var6 = zxjzList.iterator();

        while(var6.hasNext()) {
            String zxjz = (String)var6.next();
            jzList.add((new BigDecimal(zxjz.split(":")[1])).setScale(6, 4).doubleValue());
        }

        zdhcl = GetZdhcl.calcZdhcl(jzList);
        return new DoubleWritable(zdhcl);
    }

    public static double calcZdhcl(List<Double> jzList) {
        double zdhcl = 0.0D;
        double hcl = 0.0D;
        double zxjz = 0.0D;

        for(int i = 0; i < jzList.size() - 1; ++i) {
            zxjz = (Double)jzList.get(i);
            if (zxjz != 0.0D) {
                hcl = (new BigDecimal((zxjz - (Double)Collections.min(jzList.subList(i + 1, jzList.size()))) / zxjz)).setScale(6, 4).doubleValue();
                zdhcl = hcl > zdhcl ? hcl : zdhcl;
            }
        }

        return zdhcl;
    }
}

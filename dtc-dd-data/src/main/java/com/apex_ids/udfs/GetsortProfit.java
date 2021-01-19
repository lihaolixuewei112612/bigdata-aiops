package com.apex_ids.udfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.*;
import java.lang.String;
import org.apache.commons.lang.StringUtils;

/**
 * @author ChenTong
 */
@Description(
        name = "GetsortProfit",
        value = "_FUNC_(String, String) - 根据类型返回String"
)

public class GetsortProfit extends UDF {

    // private static final Logger log = LoggerFactory.getLogger(initSortProfit.class);

    public Text evaluate(Text profits, final Text field) throws Exception {
        try {
            List<String> list = Arrays.asList(profits.toString().split(";"));
            Collections.sort(list, new Comparator<String>() {
                public int compare(String o1, String o2) {
                    Double v1 = Double.parseDouble(GetsortProfit.getStringToMap(o1).get(field.toString()).toString());
                    Double v2 = Double.parseDouble(GetsortProfit.getStringToMap(o2).get(field.toString()).toString());
                    return v1.compareTo(v2);
                }
            });
            String str = StringUtils.join(list, ";");
            return new Text(str);
        } catch (Exception var5) {
            return profits;
        }
    }

    private static Map<String, Object> getStringToMap(String str) {
        String[] str1 = str.split(",");
        Map<String, Object> map = new HashMap();

        for (int i = 0; i < str1.length; ++i) {
            String[] str2 = str1[i].split(":");
            if (str2.length == 1) {
                map.put(str2[0], "");
            } else if (str2.length == 2) {
                map.put(str2[0], str2[1]);
            }
        }
        return map;
    }

}

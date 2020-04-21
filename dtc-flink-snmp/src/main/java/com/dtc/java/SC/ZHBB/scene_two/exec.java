package com.dtc.java.SC.ZHBB.scene_two;

import com.dtc.java.SC.JKZL.ExecutionEnvUtil;
import com.dtc.java.analytic.V1.common.constant.PropertiesConstants;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.operators.CoGroupOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Properties;

/**
 * @Author : lihao
 * Created on : 2020-04-14
 * @Description : 数据仓库--场景二代码
 */
public class exec {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        Map<String, String> stringStringMap = parameterTool.toMap();
        Properties properties = new Properties();
        for (String key : stringStringMap.keySet()) {
            if (key.startsWith("mysql")) {
                properties.setProperty(key, stringStringMap.get(key));
            }
        }
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String database = parameterTool.get(PropertiesConstants.MYSQL_DATABASE);
        String host = parameterTool.get(PropertiesConstants.MYSQL_HOST);
        String password = parameterTool.get(PropertiesConstants.MYSQL_PASSWORD);
        String port = parameterTool.get(PropertiesConstants.MYSQL_PORT);
        String username = parameterTool.get(PropertiesConstants.MYSQL_USERNAME);
        String alarm_rule_table = parameterTool.get(PropertiesConstants.MYSQL_ALAEM_TABLE);

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://" + host + ":" + port + "/" + database + "?useUnicode=true&characterEncoding=UTF-8";
        String sql = "select d.level_id,d.type_id,IFNULL(d.xzNum,0) as m ,IFNULL(d.wclNum,0) as n,(IFNULL(d.xzNum,0)-IFNULL(d.wclNum,0)) as f from (select b.level_id,b.type_id,b.xzNum,c.wclNum from (select a.level_id,a.type_id,count(*) as xzNum from alarm a group by a.level_id,a.type_id) b \n" +
                "left join (select a.level_id,a.type_id,count(*) as wclNum from alarm a where a.`status`!=2 group by a.level_id,a.type_id ) c on b.level_id= c.level_id) d ";
        String insert_sql ="replace into SC_ZHBB_SCRENE_TWO(riqi,level_id,type_id,zs_Num,wcl_Num,ycl_Num,js_time) values(?,?,?,?,?,?,?)";
        String sql_second = "select level_id,type_id,wcl_Num from SC_ZHBB_SCRENE_TWO where TO_DAYS(now())-TO_DAYS(riqi) =1";
        env.getConfig().setGlobalJobParameters(parameterTool);

        DataSource<Row> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setQuery(sql)
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
                .finish());

        DataSource<Row> input1 = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setQuery(sql_second)
                .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                .finish());
        CoGroupOperator<Row, Row, Row> with = input.coGroup(input1).where(new First_one()).equalTo(new First_one()).with(new MyCoGrouper());
        MapOperator<Row, Row> map = with.map(new MapFunction<Row, Row>() {
            @Override
            public Row map(Row row) throws Exception {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
                String riqi = sdf.format(System.currentTimeMillis());
                String js_time = sdf1.format(System.currentTimeMillis());
                Integer field_0 = (Integer) row.getField(0);
                Integer field_1 = (Integer) row.getField(1);
                Long field_2 = (Long) row.getField(2);
                Long field_3 = (Long) row.getField(3);
                Long field_4 = (Long) row.getField(4);
                Row row1 = new Row(7);
                row1.setField(0,riqi);
                row1.setField(1,field_0);
                row1.setField(2,field_1);
                row1.setField(3,field_2);
                row1.setField(4,field_3);
                row1.setField(5,field_4);
                row1.setField(6,js_time);
                return row1;
            }
        });

        map.output(JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(username)
                .setPassword(password)
                .setQuery(insert_sql)
                .finish());


//        map.print();

        env.execute("WordCount Example");

    }
    static class MyCoGrouper
            implements CoGroupFunction<Row,  Row, Row> {

        @Override
        public void coGroup(Iterable<Row> iVals,
                            Iterable<Row> dVals,
                            Collector<Row> out) {
            Row row1 = new Row(5);
            Long abc=0l;
            // add all Integer values in group to set
            for (Row val : iVals) {
                row1.setField(0, val.getField(0));
                row1.setField(1, val.getField(1));
                row1.setField(2, val.getField(2));
                row1.setField(3,val.getField(3));
                row1.setField(4, val.getField(4));
                abc = (Long) val.getField(3);

            }
            // multiply each Double value with each unique Integer values of group
            for (Row val : dVals) {
                Integer field = (Integer) val.getField(2);
                abc += field;
                row1.setField(3, abc);
            }
            out.collect(row1);
        }
    }

    private static class First_one implements KeySelector<Row, String> {
        @Override
        public String getKey(Row value) {
            return value.getField(0)+"_"+value.getField(1);
        }
    }

}

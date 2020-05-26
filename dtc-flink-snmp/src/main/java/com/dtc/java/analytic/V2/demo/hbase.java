package com.dtc.java.analytic.V2.demo;

import com.dtc.java.analytic.V2.common.constant.HBaseConstant;
import com.dtc.java.analytic.V2.common.model.DataStruct;
import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;

/**
 * @Author : lihao
 * Created on : 2020-05-25
 * @Description : TODO描述类作用
 */
public class hbase {
    public static void main(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//        Connection cn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("test");
//        scanTable(tableName);
        String str ="{\"time\":\"1581691002687\",\"code\":\"102_101_101_101_101.1.0\",\"host\":\"10.3.7.234\",\"nameCN\":\"磁盘剩余大小\",\"value\":\"217802544\",\"nameEN\":\"disk_free\"}";
        writeEventToHbase(str,"1");
    }
    public static void scanTable(TableName tableName) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "10.3.7.232,10.3.7.233,10.3.6.20");
        conf.set("hbase.zookeeper.property.client", "2181");
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(tableName);

        //①全表扫描
        Scan scan1 = new Scan();
        ResultScanner rscan1 = table.getScanner(scan1);
        for (Result rs : rscan1) {
            String rowKey = Bytes.toString(rs.getRow());
            System.out.println("row key :" + rowKey);
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
            }
            System.out.println("-------------------------------------------");
        }


        //②rowKey过滤器
        Scan scan2 = new Scan();
        //str$ 末尾匹配，相当于sql中的 %str  ^str开头匹配，相当于sql中的str%
        RowFilter filter = new RowFilter(CompareOperator.EQUAL, new RegexStringComparator("Key1$"));
        scan2.setFilter(filter);
        ResultScanner rscan2 = table.getScanner(scan2);
//
//        //③列值过滤器
//        Scan scan3 = new Scan();
//        //下列参数分别为列族，列名，比较符号，值
//        SingleColumnValueFilter filter3 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
//                CompareOperator.EQUAL, Bytes.toBytes("spark"));
//        scan3.setFilter(filter3);
//        ResultScanner rscan3 = table.getScanner(scan3);
//
//        //列名前缀过滤器
//        Scan scan4 = new Scan();
//        ColumnPrefixFilter filter4 = new ColumnPrefixFilter(Bytes.toBytes("name"));
//        scan4.setFilter(filter4);
//        ResultScanner rscan4 = table.getScanner(scan4);
//
//        //过滤器集合
//        Scan scan5 = new Scan();
//        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
//        SingleColumnValueFilter filter51 = new SingleColumnValueFilter(Bytes.toBytes("author"), Bytes.toBytes("name"),
//                CompareOperator.EQUAL, Bytes.toBytes("spark"));
//        ColumnPrefixFilter filter52 = new ColumnPrefixFilter(Bytes.toBytes("name"));
//        list.addFilter(filter51);
//        list.addFilter(filter52);
//        scan5.setFilter(list);
//        ResultScanner rscan5 = table.getScanner(scan5);
//
//        for (Result rs : rscan5) {
//            String rowKey = Bytes.toString(rs.getRow());
//            System.out.println("row key :" + rowKey);
//            Cell[] cells = rs.rawCells();
//            for (Cell cell : cells) {
//                System.out.println(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) + "::"
//                        + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()) + "::"
//                        + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
//            }
//            System.out.println("-------------------------------------------");
//        }
    }
    private static void writeEventToHbase(String string, String str) throws IOException {
        TableName HBASE_TABLE_NAME=null;
        String INFO_STREAM=null;
        String BAR_STREAM=null;
        if("1"==str) {
            HBASE_TABLE_NAME = TableName.valueOf("switch_1");
            //列族
            INFO_STREAM = "banka";
            //列名
            BAR_STREAM = "bk";
        }
        if("2"==str){
            HBASE_TABLE_NAME = TableName.valueOf("switch_2");
            //列族
            INFO_STREAM = "jiekou";
            //列名
            BAR_STREAM = "jk";
        }
        Configuration configuration = HBaseConfiguration.create();
//        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_QUORUM, parameterTool.get(HBaseConstant.HBASE_ZOOKEEPER_QUORUM));
//        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, parameterTool.get(HBaseConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
//        configuration.set(HBaseConstant.HBASE_RPC_TIMEOUT, parameterTool.get(HBaseConstant.HBASE_RPC_TIMEOUT));
//        configuration.set(HBaseConstant.HBASE_CLIENT_OPERATION_TIMEOUT, parameterTool.get(HBaseConstant.HBASE_CLIENT_OPERATION_TIMEOUT));
//        configuration.set(HBaseConstant.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, parameterTool.get(HBaseConstant.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_QUORUM, "10.3.7.232,10.3.7.233,10.3.6.20");
        configuration.set(HBaseConstant.HBASE_MASTER_INFO_PORT, "2181");

        configuration.set(HBaseConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, "2181");
        configuration.set(HBaseConstant.HBASE_RPC_TIMEOUT, "20000");
        Connection connect = ConnectionFactory.createConnection(configuration);
//        Admin admin = connect.getAdmin();
//        if (!admin.tableExists(HBASE_TABLE_NAME)) { //检查是否有该表，如果没有，创建
//            admin.createTable(new HTableDescriptor(HBASE_TABLE_NAME).addFamily(new HColumnDescriptor(INFO_STREAM)));
//        }
        Table table = connect.getTable(HBASE_TABLE_NAME);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();

        Put put = new Put(Bytes.toBytes(date.getTime()+"_"+1111));
        put.addColumn(Bytes.toBytes(INFO_STREAM), Bytes.toBytes(BAR_STREAM), Bytes.toBytes(2222));
        table.put(put);
        table.close();
        connect.close();
    }
}

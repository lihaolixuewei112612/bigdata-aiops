package com.dtc.java.analytic.V2.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

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
        scanTable(tableName);
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
}

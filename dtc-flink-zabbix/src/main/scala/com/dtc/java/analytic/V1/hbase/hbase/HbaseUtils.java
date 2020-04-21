package com.dtc.java.analytic.V1.hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Copyright @ 2018
 * All right reserved.
 *https://www.cnblogs.com/junrong624/p/7323483.html
 * https://blog.csdn.net/swnjut/article/details/79758226
 * @author Li Hao
 * @since 2018/12/21  15:56
 */
public class HbaseUtils {
    public static Configuration configuration;
    public static Connection connection;
    public static HBaseAdmin admin;
    public static void main(String[] args) throws IOException {
        HbaseUtils hu = new HbaseUtils();
        hu.init();
        deleteTimeRange("dtc_stream",1577186608308L,1578307705509L);

        hu.listTables();
    }
    public void init() throws IOException {
        User user = User.create(UserGroupInformation.createRemoteUser("root"));
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "10.3.7.232,10.3.7.233,10.3.7.234");
        try {
            connection = ConnectionFactory.createConnection(configuration,user);
            admin = new HBaseAdmin(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //create table
    public void createTable() throws IOException
    {
        System.out.println("[hbaseoperation] LoginServer createtable...");

        String tableNameString = "table_book";
        TableName tableName = TableName.valueOf(tableNameString);
        if (admin.tableExists(tableName))
        {
            System.out.println("[INFO] table exist");
        }
        else
        {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_1"));
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_2"));
            hTableDescriptor.addFamily(new HColumnDescriptor("columnfamily_3"));
            admin.createTable(hTableDescriptor);
        }

        System.out.println("[hbaseoperation] end createtable...");
    }

    // insert data
    public void insert() throws IOException
    {
        System.out.println("[hbaseoperation] LoginServer insert...");

        Table table = connection.getTable(TableName.valueOf("test"));
        List<Put> putList = new ArrayList<Put>();

        Put put1;
        put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("<<Java In Action>>"));
        Put put2;
        put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("event"), Bytes.toBytes("name"), Bytes.toBytes("<<C++ Prime>>"));
        Put put3;
        put3 = new Put(Bytes.toBytes("row3"));
        put3.addColumn(Bytes.toBytes("event"), Bytes.toBytes("name"), Bytes.toBytes("<<Hadoop in Action>>"));

        putList.add(put1);
        putList.add(put2);
        putList.add(put3);
        table.put(put1);

        System.out.println("[hbaseoperation] LoginServer insert...");
    }
    //queryTable
    public void queryTable() throws IOException
    {
        System.out.println("[hbaseoperation] LoginServer queryTable...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner)
        {
            byte[] row = result.getRow();
            System.out.println("row key is:" + Bytes.toString(row));

            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells)
            {
                System.out.print("family:" + Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(),cell.getFamilyLength()));
                System.out.print("qualifier:" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()));
                System.out.print("value:" + Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                System.out.println("Timestamp:" + cell.getTimestamp());
            }
        }

        System.out.println("[hbaseoperation] end queryTable...");
    }

    public void queryTableByRowKey(String rowkey) throws IOException
    {
        System.out.println("[hbaseoperation] LoginServer queryTableByRowKey...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Get get = new Get(rowkey.getBytes());
        Result result = table.get(get);

        List<Cell> listCells = result.listCells();
        for (Cell cell : listCells)
        {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
            long timestamp = cell.getTimestamp();
            String family = Bytes.toString(CellUtil.cloneFamily(cell));
            String qualifier	= Bytes.toString(CellUtil.cloneQualifier(cell));
            String value = Bytes.toString(CellUtil.cloneValue(cell));

            System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +  timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
        }

        System.out.println("[hbaseoperation] end queryTableByRowKey...");
    }


    public void queryTableByCondition(String authorName) throws IOException
    {
        System.out.println("[hbaseoperation] LoginServer queryTableByCondition...");

        Table table = connection.getTable(TableName.valueOf("table_book"));
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes("columnfamily_2"), Bytes.toBytes("author"), CompareOp.EQUAL, Bytes.toBytes(authorName));
        Scan scan = new Scan();

        scan.setFilter(filter);

        ResultScanner scanner = table.getScanner(scan);

        for (Result result : scanner)
        {
            List<Cell> listCells = result.listCells();
            for (Cell cell : listCells)

            {
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                long timestamp = cell.getTimestamp();
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                String qualifier  = Bytes.toString(CellUtil.cloneQualifier(cell));
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " + timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
            }
        }

        System.out.println("[hbaseoperation] end queryTableByCondition...");
    }

    // 删除数据
    public void deleRow(String tableName, String rowkey, String colFamily, String col) throws IOException {
        init();
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowkey));
        // 删除指定列族
        // delete.addFamily(Bytes.toBytes(colFamily));
        // 删除指定列
        // delete.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(col));
        table.delete(delete);
        // 批量删除
        /*
         * List<Delete> deleteList = new ArrayList<Delete>();
         * deleteList.add(delete); table.delete(deleteList);
         */
        table.close();
        close();
    }

    public void deleteColumnFamily(String cf) throws IOException
    {
        TableName tableName = TableName.valueOf("table_book");
        admin.deleteColumn(tableName, Bytes.toBytes(cf));
    }

    public void deleteByRowKey(String rowKey) throws IOException
    {
        Table table = connection.getTable(TableName.valueOf("table_book"));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        queryTable();
    }

    public void truncateTable() throws IOException
    {
        TableName tableName = TableName.valueOf("table_book");

        admin.disableTable(tableName);
        admin.truncateTable(tableName, true);
    }

    public void deleteTable() throws IOException
    {
        admin.disableTable(TableName.valueOf("table_book"));
        admin.deleteTable(TableName.valueOf("table_book"));
    }

    //关闭连接
    public static void close() {
        try {
            if (null != admin)
                admin.close();
            if (null != connection)
                connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    // 查看已有表
    public  void listTables() throws IOException {
        HTableDescriptor hTableDescriptors[] = admin.listTables();
        for (HTableDescriptor hTableDescriptor : hTableDescriptors) {
            System.out.println(hTableDescriptor.getNameAsString());
        }
        close();
    }
    public static void deleteTimeRange(String tableName, Long minTime, Long maxTime) {
        Table table = null;

        try {
            Scan scan = new Scan();
            scan.setTimeRange(minTime, maxTime);
            table = connection.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(scan);

            List<Delete> list = getDeleteList(rs);
//            if (list.size() > 0) {
//
//                table.delete(list);
//            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != table) {
                try {
                    table.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            if (connection != null) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    private static List<Delete> getDeleteList(ResultScanner rs) {

        List<Delete> list = new ArrayList<>();
        try {

            for (Result r : rs) {
                byte[] row = r.getRow();
                String s = new String(row);
                System.out.println(s);

//                Delete d = new Delete(r.getRow());
//                list.add(d);
            }
        } finally {
            rs.close();
        }
        return list;
    }

}

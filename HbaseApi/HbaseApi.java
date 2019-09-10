package HbaseApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseApi {
    public static Configuration conf;
    public static void main(String[] args) throws Exception {
        //
        //System.out.println(IsTableExist("student22"));
        //createTable("student33", "cfl","cf2");
//        putTable("student33", "1001","cfl","name","zhangsan");
//        putTable("student33", "1002","cfl","name","zhangsan");
//
//        putTable("student33", "1003","cfl","name","zhangsan");
        //scanTable("student33");
       // getRow("student33", "1001");
       // deleteMultiRow("student33", "1001", "1002");
        dropTable("student33");
    }

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "bigdata111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");
    }

    //judge wether the table exists or not so;
    /*

     */
    public static boolean IsTableExist(String tablename) throws IOException {
        // create a connection
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //judge wether the table exist or not so
        return admin.tableExists(tablename);
    }

    //create table
    public static void createTable(String tablename, String ... columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        if(IsTableExist(tablename)){
            System.out.println("table" + tablename + "is exist");
        }else{
            //crete table obj des
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tablename));

            //create multi column
            for(String cf:columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //by set of table
            admin.createTable(descriptor);
            System.out.println(tablename);
        }


    }

    public static void putTable(String tableName,
                                String rowKey,
                                String columnFamily,
                                String column,
                                String value) throws IOException {

        //create Htables object
        HTable hTable = new HTable(conf, tableName);
        //create a put object
        Put put = new Put(Bytes.toBytes(rowKey));
        //insert data into put object
        put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
        //data input into table
        hTable.put(put);

        hTable.close();

        System.out.println("insert great");

    }

    //scan
    public static void scanTable(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);

        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);

        for(Result result:scanner){
            Cell[] cells = result.rawCells();
            for(Cell cell:cells){
                System.out.println(Bytes.toString(cell.getRow()));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(cell.getTimestamp());
            }
        }
    }

    public static void getRowQualifier(String tableName, String rowKey, String family, String qualifier) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
    }

    public static void getRow(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();显示所有版本
//get.setTimeStamp();显示指定时间戳的版本
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }

    public static void deleteMultiRow(String tableName, String... rows) throws IOException{
        HTable hTable = new HTable(conf, tableName);
        List<Delete> deleteList = new ArrayList<Delete>();
        for(String row : rows){
            Delete delete = new Delete(Bytes.toBytes(row));
            deleteList.add(delete);
        }
        hTable.delete(deleteList);
        hTable.close();
    }
    public static void dropTable(String tableName) throws Exception{
        HBaseAdmin admin = new HBaseAdmin(conf);
        if(IsTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }
}

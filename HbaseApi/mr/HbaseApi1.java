package HbaseApi.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseApi1 {

    public static Configuration conf;

    public static void main(String[] args) throws Exception {
//判断表是否存在
//        System.out.println(isTableExist("student3"));
//创建表
//        createTable("student3","cf1","cf2");
//插入数据
//        putTableData("student3","1001","cf1","name","zhansan");
//        putTableData("student3","1002","cf1","name","zhansan");
//        putTableData("student3","1003","cf1","name","zhansan");

//        scanTable("student3");

//        getRow("student3","1001","cf1","name");


//        deleteMultiRow("student3","1001","1002");
        dropTable("student3");

    }

    /**
     * hbase的初始化
     * */
    static {
        //使用HBaseConfiguration的单例方法实例化
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","bigdata111");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");
    }

    /**
     * 判断表是否存在
     * @param tableName 表名
     *
     * */
    public static boolean isTableExist(String tableName) throws IOException {
        //在hbase中创建一个HBASE Admin
        Connection connection = ConnectionFactory.createConnection(conf);
        HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();

        //判断表是不是存在
        return admin.tableExists(tableName);
    }

    /**
     * 创建表
     * @param tableName 要创建的表名
     * @param columnFamily 列簇
     * */
    public static void createTable(String tableName,String ... columnFamily) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (isTableExist(tableName)){
            System.out.println("表"+tableName+"已将存在");
        }else {
            //创建表对象，表名需要换类型
            HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));

            //创建多个列簇
            for(String cf : columnFamily){
                descriptor.addFamily(new HColumnDescriptor(cf));
            }
            //根据表的配置，创建表
            admin.createTable(descriptor);

            System.out.println(tableName+"创建成功！");
        }
    }
    /**
     * 插入数据
     * @param tableName 表
     * @param rowKey 行键
     * @param columnFamily 列簇
     * @param column 列
     * @param value 数据
     * */
    public static void putTableData(String tableName,
                                    String rowKey,
                                    String columnFamily,
                                    String column,
                                    String value) throws IOException {
        //创建Htable的对象
        HTable hTable = new HTable(conf, tableName);
        //创建一个put对象
        Put put = new Put(Bytes.toBytes(rowKey));
        //向put对象中插入数据
        put.add(Bytes.toBytes(columnFamily),Bytes.toBytes(column),Bytes.toBytes(value));
        //数据插入表中
        hTable.put(put);

        hTable.close();

        System.out.println("数据插入成功！");
    }

    public static void scanTable(String tableName) throws IOException {
        HTable hTable = new HTable(conf, tableName);

        Scan scan = new Scan();
        ResultScanner scanner = hTable.getScanner(scan);
        for(Result result : scanner){

            Cell[] cells = result.rawCells();
            for(Cell cell : cells){
                System.out.println(Bytes.toString(cell.getRow()));
                System.out.println(Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println(Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println(cell.getTimestamp());
            }
        }
    }

    public static void getRow(String tableName, String rowKey) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();//显示所有版本
//get.setTimeStamp();显示指定时间戳的版本
//        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        Result result = table.get(get);
        for(Cell cell : result.rawCells()){
            System.out.println("行键:" + Bytes.toString(result.getRow()));
            System.out.println("列族" + Bytes.toString(CellUtil.cloneFamily(cell)));
            System.out.println("列:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
            System.out.println("值:" + Bytes.toString(CellUtil.cloneValue(cell)));
            System.out.println("时间戳:" + cell.getTimestamp());
        }
    }
    public static void getRow(String tableName, String rowKey,String family,String qualifier) throws IOException{
        HTable table = new HTable(conf, tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        //get.setMaxVersions();//显示所有版本
//get.setTimeStamp();显示指定时间戳的版本
        get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
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
        if(isTableExist(tableName)){
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("表" + tableName + "删除成功！");
        }else{
            System.out.println("表" + tableName + "不存在！");
        }
    }
}

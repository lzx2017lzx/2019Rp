package HbaseApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author Andy
 * 1、NameSpace ====>  命名空间
 * 2、createTable ===> 表
 * 3、isTable   ====>  判断表是否存在
 * 4、Region、RowKey、分区键
 */
public class HBaseUtil {


    /**
     * 初始化命名空间
     *
     * @param conf      配置对象
     * @param namespace 命名空间的名字
     * @throws Exception
     */
    public static void initNameSpace(Configuration conf, String namespace) throws Exception {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
//命名空间描述器
        NamespaceDescriptor nd = NamespaceDescriptor
                .create(namespace)
                .addConfiguration("AUTHOR", "Andy")
                .build();
//通过admin对象来创建命名空间
        admin.createNamespace(nd);
        System.out.println("已初始化命名空间");
//关闭两个对象
        close(admin, connection);
    }


    /**
     * 关闭admin对象和connection对象
     *
     * @param admin      关闭admin对象
     * @param connection 关闭connection对象
     * @throws IOException IO异常
     */
    private static void close(Admin admin, Connection connection) throws IOException {
        if (admin != null) {
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }


    /**
     * 创建HBase的表
     * @param conf
     * @param tableName
     * @param regions
     * @param columnFamily
     */
    public static void createTable(Configuration conf, String tableName, int regions, String... columnFamily) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();
//判断表
        if (isExistTable(conf, tableName)) {
            return;
        }
//表描述器 HTableDescriptor
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        for (String cf : columnFamily) {
//列描述器 ：HColumnDescriptor
            htd.addFamily(new HColumnDescriptor(cf));
        }
//htd.addCoprocessor("hbase.CalleeWriteObserver");
//创建表
        admin.createTable(htd,genSplitKeys(regions));
        System.out.println("已建表");
//关闭对象
        close(admin,connection);
    }


    /**
     * 分区键
     * @param regions region个数
     * @return splitKeys
     */
    private static byte[][] genSplitKeys(int regions) {
//存放分区键的数组
        String[] keys = new String[regions];
//格式化分区键的形式  00 01 02
        DecimalFormat df = new DecimalFormat("00");
        for (int i = 0; i < regions; i++) {
            keys[i] = df.format(i) + "";
        }


        byte[][] splitKeys = new byte[regions][];
//排序 保证你这个分区键是有序得
        TreeSet<byte[]> treeSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < regions; i++) {
            treeSet.add(Bytes.toBytes(keys[i]));
        }


//输出
        Iterator<byte[]> iterator = treeSet.iterator();
        int index = 0;
        while (iterator.hasNext()) {
            byte[] next = iterator.next();
            splitKeys[index++]= next;
        }


        return splitKeys;
    }


    /**
     * 判断表是否存在
     * @param conf      配置 conf
     * @param tableName 表名
     */
    public static boolean isExistTable(Configuration conf, String tableName) throws IOException {
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();


        boolean result = admin.tableExists(TableName.valueOf(tableName));
        close(admin, connection);
        return result;
    }
}

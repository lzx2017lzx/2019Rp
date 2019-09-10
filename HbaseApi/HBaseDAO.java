package HbaseApi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseDAO {


    private static String namespace = PropertiesUtil.getProperty("hbase.calllog.namespace");
    private static String tableName = PropertiesUtil.getProperty("hbase.calllog.tablename");
    private static Integer regions = Integer.valueOf(PropertiesUtil.getProperty("hbase.calllog.regions"));


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "bigdata111");
        conf.set("zookeeper.znode.parent", "/hbase");


        if (!HBaseUtil.isExistTable(conf, tableName)) {
            HBaseUtil.initNameSpace(conf, namespace);
            HBaseUtil.createTable(conf, tableName, regions, "f1", "f2");
        }
    }
}
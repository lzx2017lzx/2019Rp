package HbaseApi.mr;

import java.io.IOException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;


public class ReadCityMapper extends TableMapper<ImmutableBytesWritable, Put> {


    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        //将City的name和countries提取出来，相当于将每一行数据读取出来放入到Put对象中。
        Put put = new Put(key.get());
        //遍历添加column行
        for(Cell cell: value.rawCells()){
            //添加/克隆列族:cf
            if("cf".equals(Bytes.toString(CellUtil.cloneFamily(cell)))){
                //添加/克隆列：name
                if("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //将该列cell加入到put对象中
                    put.add(cell);
                    //添加/克隆列:countries
                }else if("countries".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))){
                    //向该列cell加入到put对象中
                    put.add(cell);
                }
            }
        }
        //将从city读取到的每行数据写入到context中作为map的输出
        context.write(key, put);
    }
}

package com.lzx.groupcomparable2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMap extends Mapper<LongWritable, Text, OrderBean, Text> {
    OrderBean f = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        //split
        String[] split = line.split("\t");


        //assignment
        f.setId(Integer.parseInt(split[0]));
        f.setPrice(Double.parseDouble(split[2]));



//        f.setUpflow(Long.parseLong(split[1]));
//        f.setDownflow(Long.parseLong(split[2]));
//        f.setSumflow(Long.parseLong(split[3]));
//        long upflow = Long.parseLong(split[split.length - 3]);
//        long downflow = Long.parseLong(split[split.length - 2]);

        context.write(f, new Text(split[1]));
    }
}

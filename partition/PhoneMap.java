package com.lzx.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PhoneMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //out put
        String s = value.toString();
        System.out.println(s);
        //split
        String[] as = s.split("\t");
        //cycle output
        for(String fields:as){
            //System.out.println(fields);
            context.write(new Text(fields), NullWritable.get());
        }
    }
}

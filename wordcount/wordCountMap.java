package com.lzx.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class wordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
        System.out.println(key.toString());
        //data transfer Text =>string
        String line = value.toString();
        //splite by empty
        String[] split = line.split("，");
        //output data
        for (String s:split
             ) {
            //string ->text
            context.write(new Text(s), new IntWritable(1));
        }
    }
}

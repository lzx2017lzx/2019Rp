package com.lzx.friends.mr1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapFriend extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] s1 = line.split(":");

        v.set(s1[0]);

        String[] s2 = s1[1].split(",");
        for (String friend:s2
             ) {
            k.set(friend);
            context.write(k, v);
        }
    }
}

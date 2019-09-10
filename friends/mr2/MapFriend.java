package com.lzx.friends.mr2;

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
        String[] split1 = line.split("\t");

        v.set(split1[0]);

        String[] split2 = split1[1].split(",");

        for(int i = 0; i < split2.length; i++){
            for(int j = i + 1; j < split2.length - 1; j++){
                k.set(split2[i]+"-"+split2[j]);
                context.write(k, v);
            }
        }
    }
}

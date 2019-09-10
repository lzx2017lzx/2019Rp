package com.lzx.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //initiate count
        int count = 0;
        //start count
        for (IntWritable value : values
        ) {
            count = count + value.get();
        }

        //output
        context.write(key, new IntWritable(count));
    }

}
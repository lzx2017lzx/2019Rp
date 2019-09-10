package com.lzx.output;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OutPutReduce extends Reducer<Text, NullWritable, Text, NullWritable> {
    Text k = new Text();
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        String s = key.toString();
        k.set(s+"\r\n");
        context.write(k, NullWritable.get());
    }
}

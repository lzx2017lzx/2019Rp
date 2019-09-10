package com.lzx.LogClean;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMap extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //get data
        String line = value.toString();

        //resolve log
        boolean log = parseLog(line, context);

        //judge log legel
        if(!log){
            return;
        }

        //output
        context.write(new Text(line), NullWritable.get());
    }

    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");

        //judget the length of log wether is larger than eleven
        if(fields.length > 11)
        {
            context.getCounter("map", "合法数据").increment(1);
            return true;
        }else{

            context.getCounter("map", "非法数据").increment(1);
            return false;
        }
    }
}

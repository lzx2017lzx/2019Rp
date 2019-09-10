package com.lzx.output;

import com.lzx.input.SequenceDriver;
import com.lzx.output.MyOutPutFormat;
import com.lzx.output.OutPutDriver;
import com.lzx.output.OutPutMap;
import com.lzx.output.OutPutReduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class OutPutDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"E:\\input\\lzx\\output\\log.txt", "E:\\input\\lzx\\output1"};

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OutPutDriver.class);
        job.setMapperClass(OutPutMap.class);
        job.setReducerClass(OutPutReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        //job.setInputFormatClass(MyOutPutFormat.class);
        job.setOutputFormatClass(MyOutPutFormat.class);

        //out in path
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}

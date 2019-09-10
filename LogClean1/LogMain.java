package com.lzx.LogClean1;

import com.lzx.wordcount.WordCountMain;
import com.lzx.wordcount.WordCountReduce;
import com.lzx.wordcount.wordCountMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"E:\\input\\lzx\\web.txt", "E:\\input\\lzx\\webresult\\webresultmylogclean1"};
        //get configure file
        Configuration conf = new Configuration();
        //conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        //create job task
        Job job = Job.getInstance(conf);
        job.setJarByClass(LogMain.class);

        //point map class and out type
        job.setMapperClass(LogMap.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(IntWritable.class);
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//
//        //point reduce class and reduce out data type
//        job.setReducerClass(WordCountReduce.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);


        //set partition
//        job.setPartitionerClass(WordCountPartition.class);
//        job.setNumReduceTasks(2);

        //set combine

        //job.setCombinerClass(WordCountCombine.class);
//        job.setInputFormatClass(CombineTextInputFormat.class);
//        CombineFileInputFormat.setMaxInputSplitSize(job, 4*1024*1024);
//        CombineFileInputFormat.setMinInputSplitSize(job, 2*1024*1024);

        //point data path inputed and path out
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //submit task
        job.waitForCompletion(true);
    }
}

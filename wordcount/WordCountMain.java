package com.lzx.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

public class WordCountMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //args = new String[]{"E:\\input\\lzx\\combine", "E:\\input\\wcword213467"};
        args = new String[]{"E:\\input\\lzx\\hh.txt", "E:\\input\\wcword213467dddd"};
        //args = new String[]{"E:\\wcword.txt", "E:\\wcword1"};
        //get configure file
        Configuration conf = new Configuration();
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        //create job task
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCountMain.class);

        //point map class and out type
        job.setMapperClass(wordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        //point reduce class and reduce out data type
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


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

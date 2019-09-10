package com.lzx.writablesort2.writablesort;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class SortMain {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //args = new String[]{"E:\\input\\lzx\\combine", "E:\\input\\wcword213467"};
        //args = new String[]{"E:\\input\\selfdefinepartition\\phone_data.txt", "E:\\input\\selfdefinepartition\\phonedata"};
        args = new String[]{"E:\\input\\selfdefinepartition\\GroupingComparator.txt", "E:\\input\\selfdefinepartition\\sort2"};
        //args = new String[]{"E:\\wcword.txt", "E:\\wcword1"};
        //get configure file
        Configuration conf = new Configuration();
        //conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");

        //create job task
        Job job = Job.getInstance(conf);
        job.setJarByClass(SortMain.class);

        //point map class and out type
        job.setMapperClass(SortMap.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        //job.setInputFormatClass(KeyValueTextInputFormat.class);

        //point reduce class and reduce out data type
        job.setReducerClass(SortReduce.class);
        job.setOutputKeyClass(FlowBean.class);
        job.setOutputValueClass(NullWritable.class);


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

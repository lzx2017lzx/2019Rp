package com.lzx.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class WordCountPartition extends Partitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text text, IntWritable intWritable, int i) {
        //get data
        String t = text.toString();
        //get lenght of every word
        int length = t.length();
        if(length%2 == 0){
            return 0;
        }else {
            return 1;
        }
    }
}

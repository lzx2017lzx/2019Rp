package com.lzx.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMap extends Mapper<LongWritable, Text, Text, TableBean> {
    TableBean tableBean = new TableBean();
    Text t = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //get file path
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        //files name
        String name = fileSplit.getPath().getName();
        //get data
        String line = value.toString();
        //judge add table by file's name
        if(name.equals("order.txt"))
        {
            String[] fields = line.split("\t");
            //enclosr
            tableBean.setOrder_id(fields[0]);
            tableBean.setP_id(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setFlag("0");
            tableBean.setPname("");

            t.set(fields[1]);
        }else{
            String[] fields = line.split("\t");
            //enclosr
            tableBean.setP_id(fields[0]);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("1");
            tableBean.setOrder_id("");
            tableBean.setAmount(0);

            t.set(fields[0]);
        }
        context.write(t, tableBean);

    }




}

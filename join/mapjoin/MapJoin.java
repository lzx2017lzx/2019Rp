package com.lzx.join.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class MapJoin extends Mapper<LongWritable, Text, Text, NullWritable> {

    HashMap hasmap = new HashMap<String, String>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //get cache file
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("E:\\input\\selfdefinepartition\\pd.txt"), "UTF-8"));

        //line and line read
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())){
            //split
            String[] split = line.split("\t");
            //data stored in aggraget
            hasmap.put(split[0], split[1]);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // get big table order table
        String line = value.toString();
        //split
        String[] fields = line.split("\t");

      //
        String pid = fields[1];
        if(hasmap.containsKey(pid)){
            context.write(new Text(fields[0]+"\t"+hasmap.get(pid)+"\t"+fields[2]), NullWritable.get());
        }
    }
}

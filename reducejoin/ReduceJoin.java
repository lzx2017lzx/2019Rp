package com.lzx.reducejoin;

import com.lzx.driver.Drive;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

public class ReduceJoin {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {

        args = new String[]{"E:\\input\\lzx\\reduce", "E:\\input\\lzx\\reduce\\red"};
        Drive.run(ReduceJoin.class, ReduceJoinMap.class, Text.class, TableBean.class,ReduceJoinReduce.class, TableBean.class, NullWritable.class, args[0], args[1]);
    }
}

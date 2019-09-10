package com.lzx.groupcomparable2;


import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class OrderReduce extends Reducer<OrderBean, Text,  OrderBean, Text> {
    @Override
    protected void reduce(OrderBean flowBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text text: values
        ) {
            System.out.println(text.toString());
            // out put
            context.write(flowBean, text);
        }
        System.out.println("-------------------------------");
    }
}
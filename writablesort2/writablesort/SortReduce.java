package com.lzx.writablesort2.writablesort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<FlowBean, NullWritable,  FlowBean, NullWritable> {
    @Override
    protected void reduce(FlowBean flowBean, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

//        for (Text text: values
//        ) {
//            // out put
//            context.write(text, flowBean);
//        }

        context.write(flowBean, NullWritable.get());
    }
}

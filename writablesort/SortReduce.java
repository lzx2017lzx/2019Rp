package com.lzx.writablesort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<FlowBean, Text, Text, FlowBean> {
    @Override
    protected void reduce(FlowBean flowBean, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for (Text text: values
        ) {
            // out put
            context.write(text, flowBean);
        }


    }
}

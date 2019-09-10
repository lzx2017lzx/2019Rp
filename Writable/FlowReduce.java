package com.lzx.Writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReduce extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // initiate two count, calculate upflow sum, downflow sum
        long sumupflow = 0;
        long sumdownflow = 0;
        for (FlowBean f: values
             ) {
            sumupflow += f.getUpflow();
            sumdownflow += f.getDownFlow();
        }

        // out put
        context.write(key, new FlowBean(sumupflow, sumdownflow));
    }
}

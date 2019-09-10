package com.lzx.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.OutputStream;

public class MyRecordWriter extends RecordWriter<Text, NullWritable> {

    FSDataOutputStream itstarout = null;
    FSDataOutputStream otherout = null;
    public MyRecordWriter(TaskAttemptContext context){
        // get director in job
        Path outputPath = FileOutputFormat.getOutputPath(context);
        // get filesystem
        FileSystem fs = null;
        try{
            fs = FileSystem.get(context.getConfiguration());

            //create two flow
            itstarout = fs.create(new Path(outputPath,"itstar.log"));
            otherout = fs.create(new Path(outputPath,"other.log"));
        }catch(Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        // judge wether contail itstart field
        if(text.toString().contains("itstar")){
            itstarout.write(text.toString().getBytes());
        }else{
            otherout.write(text.toString().getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //close resource
        if(itstarout != null){
            itstarout.close();
        }

        if(otherout != null){
            otherout.close();
        }
    }
}

package com.lzx.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private Configuration configuration;
    private FileSplit split;

    //wether handle data
    private boolean processed = false;

    private BytesWritable value = new BytesWritable();


    /*
    this is initialize
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        configuration = context.getConfiguration();
        split = (FileSplit)inputSplit;


    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!processed)
        {
            //define data stored buffer
            byte[] contents = new byte[(int)split.getLength()];

            FileSystem fs = null;
            FSDataInputStream fis = null;

            try{
                // get file system
                Path path = split.getPath();
                fs = ((Path) path).getFileSystem(configuration);

                // create stream for read data
                fis = fs.open(path);

                // read file
                IOUtils.readFully(fis, contents, 0, contents.length);

                // write file
                value.set(contents, 0, contents.length);

            }catch(Exception e){

            }finally {
                IOUtils.closeStream(fis);
            }
            // not repeat read
            processed = true;
            return true;

        }


        return false;
    }


    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ?1:0;
    }

    @Override
    public void close() throws IOException {

    }
}

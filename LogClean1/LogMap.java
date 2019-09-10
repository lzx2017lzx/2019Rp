package com.lzx.LogClean1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogMap extends Mapper<LongWritable, Text, NullWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //get data
        String line = value.toString();
        //resolve log
        LogBean log = ProcessLog(line);
        if(log.isVail())
        {
            context.write(NullWritable.get(), new Text(log.toString()));
        }

    }

    private LogBean ProcessLog(String line) {
        LogBean logBean = new LogBean();
        //split
        String[] fields = line.split(" ");
        if(fields.length > 11)
        {
            logBean.setAddr(fields[0]);
            logBean.setUser(fields[1]);
            logBean.setTime(fields[3].substring(1));
            logBean.setRequest(fields[6]);
            logBean.setStatus(fields[8]);
            logBean.setSize(fields[9]);
            logBean.setReferer(fields[10]);
            if(fields.length > 12)
            {
                logBean.setUser_agent(fields[11] + fields[12]);
            }else{
                logBean.setUser_agent(fields[11]);
            }

            if(Integer.parseInt(logBean.getStatus()) >= 400){
                logBean.setVail(false);
            }


        }else{
            logBean.setVail(false);
        }

        return logBean;
    }
}

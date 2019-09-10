package com.lzx;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext){
        this.context = processorContext;
    }

    @Override
    public void process(byte[] bytes, byte[] line){
        //data handle logic
        String input = new String(line);

        if(input.contains("MOVIE_RATING_PREFIX:")){
            input = input.split("MOVIE_RATING_PREFIX:")[1].trim();

            System.out.println("process data");

            context.forward("logProcessor".getBytes(),input.getBytes());

        }
    }

    @Override
    public void punctuate(long l){

    }

    @Override
    public void close(){

    }
}

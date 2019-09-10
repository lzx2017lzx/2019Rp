package com.lzx.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

import static com.lzx.compress.CompressDriver.compress;

public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        //compress("E:\\input\\selfdefinepartition\\jdk_1.8.0.0_64.exe", "org.apache.hadoop.io.compress.GzipCodec");
        compress("E:\\input\\selfdefinepartition\\jdk_1.8.0.0_64.exe", "org.apache.hadoop.io.compress.BZip2Codec");

        //decompression("E:\\input\\selfdefinepartition\\jdk_1.8.0.0_64.exe.gz", ".txt");
    }
    /*
    decompress
     */
    public static void Compress(String filename, String method) throws IOException, ClassNotFoundException {
        //create inputflow
        FileInputStream fis = new FileInputStream(new File(filename));

        //define de/com
        Class codeClass = Class.forName(method);

        //by tool class get dec obj, at the same time set hadoop
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass, new Configuration());

        //create a output flow
        FileOutputStream fos = new FileOutputStream(new File(filename + codec));

        //decompre flow
        CompressionOutputStream cos = codec.createOutputStream(fos);

        IOUtils.copyBytes(fis, fos, 1024*1024*5, false);

        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cos);

    }

    public static void decompression(String filename, String name) throws IOException {
        //get instance of de
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        //input path of decompress
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

        //out put flow
        FileOutputStream fos = new FileOutputStream(new File(filename + name));

        IOUtils.copyBytes(cis, fos, 1024*1024*5, false);



    }
}

package com.lzx.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import sun.reflect.Reflection;

import java.io.*;
import java.lang.reflect.ReflectPermission;

public class CompressDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException {

//        compress("F:\\input\\plus\\新建文件夹\\phone.txt","org.apache.hadoop.io.compress.BZip2Codec");
        decompression("F:\\input\\plus\\新建文件夹\\phone.txt.bz2","txt");
    }
    /**
     * 压缩
     * @param filename 文件路径+文件名
     * @param method 解码器
     *
     * */

    public static void compress(String filename,String method) throws IOException, ClassNotFoundException {

        //创建输入流

        FileInputStream fis = new FileInputStream(new File(filename));

        //通过反射找到解码器
        Class codeClass = Class.forName(method);

        //通过反射工具列找到解码器的对象，需要配置Hadoop的conf
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codeClass,new Configuration());

        //创建输出流
        FileOutputStream fos= new FileOutputStream(new File(filename+codec.getDefaultExtension()));

        //获取解码器的输出对象
        CompressionOutputStream cos = codec.createOutputStream(fos);

        //对接流
        IOUtils.copyBytes(fis,cos,1024*1024*5,true);

    }
    //解压缩
    public static void decompression(String filename,String decoded) throws IOException {

        //获取factory实例
        CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = factory.getCodec(new Path(filename));
        //配置解压缩的输入
        CompressionInputStream cis = codec.createInputStream(new FileInputStream(new File(filename)));

        //输出流
        FileOutputStream fos = new FileOutputStream(new File(filename+"."+decoded));

        //流拷贝
        IOUtils.copyBytes(cis,fos,1024*1024*5,true);




    }

}

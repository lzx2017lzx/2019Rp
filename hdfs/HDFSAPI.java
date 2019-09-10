package com.lzx.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSAPI {
    //get hdfs
    @Test
    public void initHDFS()throws IOException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(conf);

        //print file system
        System.out.println(fs.toString());
        System.out.println("hello lllll");
        System.out.println("你好");
    }

    /**
     * /upload
     */


    @Test
    public void putHDFS()throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();
        //set temparare argumente
        conf.set("dfs.replication", "2");

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //windows path would be upload
        Path input = new Path("E:\\word.txt");

        Path output = new Path("hdfs://bigdata111:9000/word.txt");

       //use copy way
        fs.copyFromLocalFile(input, output);

        //close
        fs.close();

        System.out.println("upload great.");
    }

    /*
    download
     */
    @Test
    public void getHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //download file
        //1.wether to delete the src file, path download src, path download dst, check or not so
        fs.copyToLocalFile(false, new Path("hdfs://bigdata111:9000/word.txt"), new Path("E:\\output.txt"), true);

        //close
        fs.close();

        System.out.println("download great");


    }


    /*
    create directory
     */
    @Test
    public void mkdirHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //create directory
        fs.mkdirs(new Path("hdfs://bigdata111:9000/idea/bb"));

        //close
        fs.close();
        System.out.println("create bb directory.");
    }

    /*
    delete directory
     */
    @Test
    public void deleteHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //delete directory
        fs.delete(new Path("hdfs://bigdata111:9000/idea/bb"), false);

        //close
        fs.close();
        System.out.println("delete bb directory.");
    }

    /*
    rename
     */
    @Test
    public void renameHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //rename
        fs.rename(new Path("hdfs://bigdata111:9000/idea"), new Path("hdfs://bigdata111:9000/idealala"));

        //close
        fs.close();
    }

    /*
    query file's detail info
     */
    @Test
    public void readFileHDFS() throws URISyntaxException, IOException, InterruptedException {
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //iterator
        RemoteIterator<LocatedFileStatus> listfiles = fs.listFiles(new Path("hdfs://bigdata111:9000/"), true);

//
//        //go through
//        while(listfiles.hasNext())
//        {
//            LocatedFileStatus filestatus = listfiles.next();
//
//            //file name
//            System.out.println(filestatus.getPath().getName());
//
//            //block size
//            System.out.println(filestatus.getBlockSize());
//
//            //authority
//            System.out.println(filestatus.getPermission());
//
//            BlockLocation[] blockLocation = filestatus.getBlockLocations();
//
//        }
        FileStatus[]  liststatus = fs.listStatus(new Path("/"));
        for(FileStatus status: liststatus)
        {
            //judge weather it is a file
            if(status.isFile())
            {
                System.out.println("file:" + status.getPath().getName());
            }else{
                System.out.println("directory:" + status.getPath().getName());
            }
        }

        //
        fs.close();

    }

    /*
    by io upload file
     */
    @Test
    public void putFileHDFS() throws URISyntaxException, IOException, InterruptedException {
        //create configure file
        Configuration conf = new Configuration();

        //get file system
       FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //create input flow
        FileInputStream fis = new FileInputStream(new File("E:\\新建文本文档.txt"));

        //out path
        Path path = new Path("hdfs://bigdata111:9000/idea/temp.txt");
        FSDataOutputStream fos = fs.create(path);

        //connect input and output
        try {
            IOUtils.copyBytes(fis, fos, 4 * 1024, false);
        }catch(IOException e)
        {
            e.printStackTrace();
        }finally{
            IOUtils.closeStream(fos);
            IOUtils.closeStream(fis);
            fs.close();
            //sout
            System.out.println("upload great.");
        }

    }

    @Test
    public void getFileHDFS() throws URISyntaxException, IOException, InterruptedException {
        //create configure file
        Configuration conf = new Configuration();

        //get file system
        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        //create input flow
        Path path = new Path("hdfs://bigdata111:9000/idea/temp.txt");
        FSDataInputStream fis = fs.open(path);

        //output contrl
        IOUtils.copyBytes(fis, System.out, 4*1024, true);

    }

    @Test
    public void readFileSeek1() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        Path path = new Path("hdfs://bigdata111:9000/hadoop-2.8.4.tar.gz");

        FSDataInputStream fis = fs.open(path);

        FileOutputStream fos = new FileOutputStream("E:\\a");

        byte[] buf = new byte[1024];

        for(int i = 0; i < 128 * 1024; i++){
            fis.read(buf);
            fos.write(buf);
        }

        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
    }

    @Test
    public void readFileSeek2() throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://bigdata111:9000"), conf, "root");

        Path path = new Path("hdfs://bigdata111:9000/hadoop-2.8.4.tar.gz");

        FSDataInputStream fis = fs.open(path);

        FileOutputStream fos = new FileOutputStream("E:\\b");

        //spot offset
        fis.seek(128*1024*1024);
//        byte[] buf = new byte[1024];
//
//        for(int i = 0; i < 128 * 1024; i++){
//            fis.read(buf);
//            fos.write(buf);
//        }

        IOUtils.copyBytes(fis, fos, 4*1024, true);
    }
    /*
    consistence
     */

    @Test
    public void writeFile() throws IOException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(configuration);

        //create file output
        Path path = new Path("E:\\output.txt");
        FSDataOutputStream fos = fs.create(path);

        fos.write("this is first hello world.".getBytes());
        fos.hflush();
        fos.close();
    }



}

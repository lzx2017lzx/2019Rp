package com.lzx.join.mapjoin;

import com.lzx.driver.Drivebc;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

public class MapJoinMain {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        args = new String[]{"E:\\input\\selfdefinepartition\\order.txt", "E:\\input\\selfdefinepartition\\mapper", "file:///E:/input/selfdefinepartition/pd.txt"};
        Drivebc.run(MapJoinMain.class, MapJoin.class, Text.class, NullWritable.class, 0, args[0], args[1], args[2]);

    }
}

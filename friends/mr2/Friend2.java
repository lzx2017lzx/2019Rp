package com.lzx.friends.mr2;

import com.lzx.driver.Drive;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

public class Friend2 {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        args = new String[]{"E:\\input\\lzx\\friendsmr1", "E:\\input\\lzx\\friendsmr2"};
        Drive.run(Friend2.class, MapFriend.class, Text.class, Text.class, ReduceFriend.class, Text.class, Text.class, args[0], args[1]);
    }
}

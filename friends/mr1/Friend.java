package com.lzx.friends.mr1;

import com.lzx.driver.Drive;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.net.URISyntaxException;

public class Friend {
    public static void main(String[] args) throws ClassNotFoundException, URISyntaxException, InterruptedException, IOException {
        args = new String[]{"E:\\input\\lzx\\friends.txt", "E:\\input\\lzx\\friendsmr1"};
        Drive.run(Friend.class, MapFriend.class, Text.class, Text.class, ReduceFriend.class, Text.class, Text.class, args[0], args[1]);
    }
}

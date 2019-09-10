package com.lzx.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Arrays;

public class PhonePartition extends Partitioner<Text, NullWritable> {
    //定义移动的规则
    public static final String[] YD = {
            "134","135","136",
            "137","138","139",
            "150","151","152",
            "157","158","159",
            "188","187","182",
            "183","184","178",
            "147","172","198",
    };
    //定义电信的规则
    public static final String[] DX = {
            "133","149","153",
            "173","177","180",
            "181","189","199"};
    //定义联通的规则
    public static final String[] LT = {
            "130","131","132",
            "145","155","156",
            "166","171","175",
            "176","185","186","166"
    };


    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {

        //get data
        String fields = text.toString();
        //get the head three bit
        String substring = fields.substring(0, 3);
        //judge which partition
        if(Arrays.asList(YD).contains(substring)){
            return 0;
        }else if(Arrays.asList(DX).contains(substring))
        {
            return 1;
        }else if(Arrays.asList(LT).contains(substring))
        {
            return 2;
        }
        return 3;
    }
}

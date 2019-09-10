package com.lzx.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        //create agraggation, store order table object
        ArrayList<TableBean> orderbeans = new ArrayList<TableBean>();
        //store production object
        TableBean pdbean = new TableBean();

        for (TableBean bean:values
             ) {
            // judge wether it is order table
            if("0".equals(bean.getFlag())){
                //define store order.txt object
                TableBean orderbean = new TableBean();

                try {
                    BeanUtils.copyProperties(orderbean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderbeans.add(orderbean);
            }else{
                //copy product talbe into memory
                try {
                    BeanUtils.copyProperties(pdbean, bean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("====================================");
        }
        System.out.println("===================delimits================");

        for (TableBean bean2:orderbeans
        ) {
            //transfer product name into order table
            bean2.setPname(pdbean.getPname());

            //data output
            context.write(bean2,NullWritable.get());
        }
    }
}

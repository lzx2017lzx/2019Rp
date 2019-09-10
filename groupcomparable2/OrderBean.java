package com.lzx.groupcomparable2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }


    private int id;
    private double price;


    public OrderBean(){

    }

    public int compareTo(OrderBean o) { //-1 not exchange position
        //return this.sumflow > o.sumflow?-1:1;
        if(this.id > o.id)
        {
            return 1;
        }else if(this.id < o.id){
            return -1;
        }else{
            return this.price > o.price?-1:1;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeDouble(this.price);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return this.id + "\t" + this.price;
    }
}

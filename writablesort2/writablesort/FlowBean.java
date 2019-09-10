package com.lzx.writablesort2.writablesort;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean> {
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


    //anti deti
    public FlowBean(){

    }

    //
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    //deti
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.id);
        dataOutput.writeDouble(this.price);
    }

    //compare
    public int compareTo(FlowBean o) {
        //-1 not exchange position
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

    @Override
    public String toString() {
        return this.id + "\t" + this.price;
    }
}



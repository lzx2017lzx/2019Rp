package com.lzx.writablesort;

import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Getter
@Setter
public class FlowBean implements WritableComparable<FlowBean> {
    private long upflow;
    private long downflow;
    private long sumflow;

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDownflow() {
        return downflow;
    }

    public void setDownflow(long downflow) {
        this.downflow = downflow;
    }

    public long getSumflow() {
        return sumflow;
    }

    public void setSumflow(long sumflow) {
        this.sumflow = sumflow;
    }

    //anti deti
    public FlowBean(){

    }

    //
    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumflow = dataInput.readLong();
    }

    //deti
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.downflow);
        dataOutput.writeLong(this.sumflow);
    }

    //compare
    public int compareTo(FlowBean o) {
        //-1 not exchange position
        return this.sumflow > o.sumflow?-1:1;
    }

    @Override
    public String toString() {
        return this.upflow + "\t" + this.downflow + "\t" + this.sumflow;
    }
}



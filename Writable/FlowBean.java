package com.lzx.Writable;

import com.sun.tools.javac.comp.Flow;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Getter
@Setter
public class FlowBean implements Writable {
    public Long getUpflow() {
        return upflow;
    }

    public void setUpflow(Long upflow) {
        this.upflow = upflow;
    }

    public Long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Long downFlow) {
        this.downFlow = downFlow;
    }

    public Long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Long sumFlow) {
        this.sumFlow = sumFlow;
    }

    private Long upflow;
    private Long downFlow;
    private Long sumFlow;

    // no argument construct function
    public FlowBean(){

    }

    public FlowBean(long upflow, long downFlow)
    {
        this.upflow = upflow;
        this.downFlow = downFlow;
        this.sumFlow = upflow + downFlow;
    }

    //deria
    public void write(DataOutput output) throws IOException {
        output.writeLong(upflow);
        output.writeLong(downFlow);
        output.writeLong(sumFlow);
    }

    //and deri
    public void readFields(DataInput input) throws IOException {
        this.upflow = input.readLong();
        this.downFlow = input.readLong();
        this.sumFlow = input.readLong();
    }

    @Override
    public String toString() {
        return this.upflow + "\t" + this.downFlow + "\t" + this.sumFlow;
    }
}

package com.hedeoer.mr.comparable;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*
    自定义排序：
    1.自定义一个类并实现WritableComparable接口
    2.WritableComparable接口继承了 Writable和Comparable
    3.重写方法
 */
public class FlowBean implements WritableComparable<FlowBean>{

    private long upFlow;//上行流量
    private long downFlow;//下行流量
    private long sumFlow; //总流量

    public FlowBean() {
    }
    /*
        用来指定排序方式
     */
    @Override
    public int compareTo(FlowBean o) {
        //按照总流量进行排序
        return -Long.compare(this.sumFlow,o.sumFlow);
    }

    /*
        序列化时会调用此方法
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /*
        反序列化时会调用此方法
        注意：序列化时的顺序和反序列化时的顺序需要保持一致
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }
    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }



}

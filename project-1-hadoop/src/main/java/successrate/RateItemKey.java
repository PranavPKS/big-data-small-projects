package successrate;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RateItemKey implements Writable, WritableComparable<RateItemKey> {
    private DoubleWritable rate = new DoubleWritable();
    private IntWritable item = new IntWritable();

    public RateItemKey() {
    }

    public void set(double r, int i){
        this.rate.set(r);
        this.item.set(i);
    }

    public int compareTo(RateItemKey o) {
        int compareValue = this.rate.compareTo(o.getRate()) * -1;
        if (compareValue == 0){
            compareValue = this.item.compareTo(o.getItem());
        }

        return compareValue;
    }

    public void write(DataOutput dataOutput) throws IOException {
        rate.write(dataOutput);
        item.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        rate.readFields(dataInput);
        item.readFields(dataInput);
    }

    public DoubleWritable getRate() {
        return rate;
    }

    public IntWritable getItem() {
        return item;
    }
}

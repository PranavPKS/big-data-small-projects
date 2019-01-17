package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountTokenKey implements Writable, WritableComparable<CountTokenKey> {
    private IntWritable count = new IntWritable();
    private Text token = new Text();

    public CountTokenKey() {
    }

    public  void set(int c, String token){
        this.count.set(c);
        this.token.set(token);
    }

    public int compareTo(CountTokenKey o) {
        int compareValue = this.count.compareTo(o.getCount()) * -1;
        if (compareValue == 0){
            compareValue = this.token.compareTo(o.getToken());
        }

        return compareValue;
    }

    public void write(DataOutput dataOutput) throws IOException {
        count.write(dataOutput);
        token.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        count.readFields(dataInput);
        token.readFields(dataInput);
    }

    public IntWritable getCount() {
        return count;
    }

    public Text getToken() {
        return token;
    }
}

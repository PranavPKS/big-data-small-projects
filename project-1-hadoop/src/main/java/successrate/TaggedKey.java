package successrate;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {
    public TaggedKey() {
    }

    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    public void set(String key, int tag){
        this.joinKey.set(key);
        this.tag.set(tag);
    }

    public int compareTo(TaggedKey taggedKey){
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());

        if (compareValue == 0){
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }

        return compareValue;
    }

    public TaggedKey read(DataInput in) throws IOException {
        TaggedKey taggedKey = new TaggedKey();
        taggedKey.readFields(in);
        return taggedKey;
    }
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        tag.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        tag.readFields(in);
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public IntWritable getTag() {
        return tag;
    }
}

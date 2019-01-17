
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedJoiningGroupingComparator extends WritableComparator {
    public TaggedJoiningGroupingComparator(){
        super(TaggedKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b){
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;

        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}
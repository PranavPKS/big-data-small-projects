
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class JoiningReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {
    private Text joinedText = new Text();
    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {


        int count = 0, click = 0, buy = 0;
        for(Text value : values){
            if (count == 0)
                buy = Integer.parseInt(value.toString());
            else
                click = Integer.parseInt(value.toString());
            count += 1;
        }

        if (count != 2) return;
        double rate = 0.0;
        if (click != 0 && buy != 0)
            rate = buy * 1.0 / click;

        StringBuilder builder = new StringBuilder();
        builder.append(key.getJoinKey()).append(",");
        builder.append(Double.toString(rate));
        joinedText.set(builder.toString());
        context.write(nullKey, joinedText);
    }
}

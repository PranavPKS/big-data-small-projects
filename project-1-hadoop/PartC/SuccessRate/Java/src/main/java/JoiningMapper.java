
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.StringTokenizer;

public class JoiningMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
    private static final Log LOG = LogFactory.getLog(JoiningMapper.class);
    private TaggedKey taggedKey = new TaggedKey();
    private Text data = new Text();
    private int joinOrder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        if (fileSplit.getPath().toString().contains("buy-count"))
            joinOrder = Integer.parseInt(context.getConfiguration().get("buy-count/"));
        else
            joinOrder = Integer.parseInt(context.getConfiguration().get("clicks-count/"));

        if (joinOrder < 0)
            joinOrder = -1* joinOrder;
//        LOG.info("joinOrder" + Integer.toString(joinOrder));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException{
        StringTokenizer itr = new StringTokenizer(value.toString());
        String joinKey = itr.nextToken();
        String values = itr.nextToken();

        taggedKey.set(joinKey, joinOrder);

        data.set(values);
        context.write(taggedKey, data);
    }
}

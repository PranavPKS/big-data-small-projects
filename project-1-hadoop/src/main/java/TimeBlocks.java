import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.StringTokenizer;

public class TimeBlocks {

    public static class TimeRevenueMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final static LongWritable rev = new LongWritable();
        private final static Text bucket = new Text();
        private Logger logger = Logger.getLogger(TimeRevenueMapper.class);
        private String timestampToBucket(String str) {
            return str.substring(11, 13);
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
//            logger.info(line);
            StringTokenizer itr = new StringTokenizer(line, ",");
            String sid = itr.nextToken();
            String timestamp = itr.nextToken();
//            logger.info(timestamp);
            String item = itr.nextToken();

            // TODO: Check with GCA about this

            long price = Long.parseLong(itr.nextToken());
            long quantity = Long.parseLong(itr.nextToken());
            rev.set(price * quantity);

            String bucketStr = timestampToBucket(timestamp);
//            logger.info(bucketStr);
            bucket.set(bucketStr);
            context.write(bucket, rev);
        }
    }

    public static class RevenueSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        private final static LongWritable totalRev = new LongWritable();

        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long total = 0;
            for(LongWritable val : values)
                total += val.get();

            totalRev.set(total);
            context.write(key, totalRev);

        }
    }

    public static class RevenueBucketMapper extends Mapper<Object, Text, LongWritable, Text> {
        private final static Text bucket = new Text();
        private final static LongWritable rev = new LongWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            String bucketStr = itr.nextToken();
            long revenue = -1*Long.parseLong(itr.nextToken());

            rev.set(revenue);
            bucket.set(bucketStr);
            context.write(rev, bucket);
        }
    }

    public static class RevenueBucketSortReducer extends Reducer<LongWritable, Text, Text, LongWritable>{
        public void reduce(LongWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            key.set(key.get() * -1);

            for (Text value : values){
                context.write(value, key);
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "BucketRevenue");
        job1.setJarByClass(TimeBlocks.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(LongWritable.class);

        job1.setMapperClass(TimeRevenueMapper.class);
        job1.setCombinerClass(RevenueSumReducer.class);
        job1.setReducerClass(RevenueSumReducer.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.textoutputformat.separator", "\t");
        Job job2 = Job.getInstance(conf2, "Sort Revenue");
        job2.setJarByClass(TimeBlocks.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setMapperClass(RevenueBucketMapper.class);
        job2.setReducerClass(RevenueBucketSortReducer.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}

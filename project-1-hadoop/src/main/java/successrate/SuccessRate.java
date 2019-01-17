package successrate;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class SuccessRate {

    public static class ItemMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text item = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            itr.nextToken();
            itr.nextToken();
            String itemStr = itr.nextToken();

            item.set(itemStr);
            context.write(item, one);
        }
    }

    public static class ItemSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TopKMapper extends Mapper<Object, Text, RateItemKey, Text>{

        private RateItemKey keyOut = new RateItemKey();
        private Text valueOut = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException{
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            String item = itr.nextToken();
            String rate = itr.nextToken();

            keyOut.set(Double.parseDouble(rate), Integer.parseInt(item));
            valueOut.set(item);
            context.write(keyOut, valueOut);
        }
    }

    public static class TopKReducer extends Reducer<RateItemKey, Text, NullWritable, Text>{
        private int k = 0;
        private NullWritable keyOut = NullWritable.get();
        public void reduce(RateItemKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException{
            if (k > 10) return;

            for(Text val : values){
                k += 1;
                context.write(keyOut, val);

                if (k > 9)
                    break;
            }
        }
    }



    public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text>{

        public int getPartition(TaggedKey taggedKey, Text text, int numPartitions){
            return taggedKey.getJoinKey().hashCode() % numPartitions;
        }
    }



    public static void main(String[] args) throws Exception {
        for(int i = 0; i < 2; i++) {
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Counts");
            job.setJarByClass(SuccessRate.class);

            job.setMapperClass(ItemMapper.class);
            job.setCombinerClass(ItemSumReducer.class);
            job.setReducerClass(ItemSumReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            FileInputFormat.addInputPath(job, new Path(args[i]));
            FileOutputFormat.setOutputPath(job, new Path(args[i+2]));
            job.waitForCompletion(true);
        }

        Splitter splitter = Splitter.on('/');

        StringBuilder filePaths = new StringBuilder();
        Configuration conf3 = new Configuration();

        for(int i = 2; i < args.length - 2; i++){
            String fileName = Iterables.getLast(splitter.split(args[i]));
            conf3.set(fileName, Integer.toString(i - 1));
            filePaths.append(args[i]).append(",");
        }

        filePaths.setLength(filePaths.length() - 1);

        Job job3 = Job.getInstance(conf3, "ReduceSideJoin");

        job3.setJarByClass(SuccessRate.class);

        FileInputFormat.addInputPaths(job3, filePaths.toString());
        FileOutputFormat.setOutputPath(job3, new Path(args[args.length-2]));

        job3.setMapperClass(JoiningMapper.class);
        job3.setReducerClass(JoiningReducer.class);
        job3.setPartitionerClass(TaggedJoiningPartitioner.class);
        job3.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
        job3.setOutputKeyClass(TaggedKey.class);
        job3.setOutputValueClass(Text.class);

        job3.waitForCompletion(false);

        Configuration conf4 = new Configuration();
        Job job4 = Job.getInstance(conf4, "Top 10 items");
        job4.setJarByClass(SuccessRate.class);

        job4.setNumReduceTasks(1);
        job4.setMapperClass(TopKMapper.class);
        job4.setReducerClass(TopKReducer.class);

        job4.setMapOutputKeyClass(RateItemKey.class);
        job4.setMapOutputValueClass(Text.class);

        job4.setOutputKeyClass(NullWritable.class);
        job4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job4, new Path(args[args.length-2]));
        FileOutputFormat.setOutputPath(job4, new Path(args[args.length-1]));

        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}

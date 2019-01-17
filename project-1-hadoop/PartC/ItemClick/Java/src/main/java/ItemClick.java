import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


public class ItemClick {



    public static class ItemMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        private boolean isMonthApril(String str) {
            String[] tokens = str.split("-");
            return tokens[1].equals("04");
        }

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");

            if (isMonthApril(tokens[1])) {
                word.set(tokens[2]);
                context.write(word, one);
            }
        }
    }

    public static class ItemCountMapper extends Mapper<Object, Text, IntWritable, Text> {
        private final static IntWritable keyOut = new IntWritable();
        private static final Log logger = LogFactory.getLog(ItemMapper.class);

        private Text word = new Text();


        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            logger.info(value.toString());
            StringTokenizer itr = new StringTokenizer(value.toString());
            String[] tokens = new String[]{"", ""};
            int i = 0;
            while (i < 2){
                tokens[i] = itr.nextToken();
                i += 1;
            }

            int val = Integer.parseInt(tokens[1]);
            keyOut.set(-1 * val);
            value.set(tokens[0]);
            context.write(keyOut, value);
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

    public static class TopItemCountReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        int count = 0;

        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            if (count >= 10) return;
            key.set(key.get() * -1);

            for (Text value : values) {
                count += 1;
                context.write(value, key);
                if (count == 10) break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "Item Count");
        job1.setJarByClass(ItemClick.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        job1.setMapperClass(ItemMapper.class);
        job1.setCombinerClass(ItemSumReducer.class);
        job1.setReducerClass(ItemSumReducer.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.textoutputformat.separator", "\t");
        Job job2 = Job.getInstance(conf2, "Top items");
        job2.setJarByClass(ItemClick.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        job2.setMapperClass(ItemCountMapper.class);
        job2.setReducerClass(TopItemCountReducer.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "word count");

        job1.setJarByClass(WordCount.class);
        job1.setMapperClass(WordCountTopK.TokenizerMapper.class);
        job1.setCombinerClass(WordCountTopK.IntSumReducer.class);
        job1.setReducerClass(WordCountTopK.IntSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);
    }
}

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCountTopK {
    private static Set<String> stopWords = new HashSet<String>(Arrays.asList("a","about","above","after","again",
            "against","all","am","an","and","any","are","arent","as","at","be","because","been","before",
            "being","below","between","both","but","by","cant","cannot","could","couldnt","did","didnt","do",
            "does","doesnt","doing","dont","down","during","each","few","for","from","further","had",
            "hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers",
            "herself","him","himself","his","how","hows","i","id","ill","im","ive","if","in","into","is",
            "isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor",
            "not","of","off","on","once","only","or","other","ought","our","ours", "ourselves","out","over","own",
            "same","shant","she","shed","shell","shes","should","shouldnt","so","some","such","than","that","thats",
            "the","their","theirs","them","themselves","then","there","theres","these","they","theyd","theyll","theyre",
            "theyve","this","those","through","to","too","under","until","up","very","was","wasnt","we","wed","well",
            "were","weve","were","werent","what","whats","when","whens","where","wheres","which","while","who","whos",
            "whom","why","whys","with","wont","would","wouldnt","you","youd","youll","youre","youve","your","yours",
            "yourself","yourselves"));

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token = itr.nextToken();

                token = token.replaceAll("[^a-zA-Z]", "");
                token = token.toLowerCase();
                if (token.length() == 0 || stopWords.contains(token))
                    continue;
                word.set(token);
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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

    public static class TopKMapper extends Mapper<Object, Text, CountTokenKey, Text> {
        private CountTokenKey keyOut = new CountTokenKey();
        private Text valueOut = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int c = 0;
            while(itr.hasMoreTokens()){
                String token = itr.nextToken();
                if (c == 0) {
                    valueOut.set(token);
                } else {
                    int count = Integer.parseInt(token);
                    keyOut.set(count, valueOut.toString());
                }
                c += 1;
            }


            context.write(keyOut, valueOut);

        }
    }

    public static class TopKReducer extends Reducer<CountTokenKey, Text, Text, IntWritable> {
        private int count = 0;
        private IntWritable valueOut = new IntWritable();
        public void reduce(CountTokenKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            if (count >= 2000)
                return;

            int kk = key.getCount().get();
            valueOut.set(kk);
            for(Text val: values){
                count += 1;
                context.write(val, valueOut);
                if (count >= 2000)
                    break;
            }

        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "word count");

        job1.setJarByClass(WordCountTopK.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.textoutputformat.separator", "\t");

        Job job2 = Job.getInstance(conf1, "top k");

        job2.setJarByClass(WordCountTopK.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(CountTokenKey.class);
        job2.setMapOutputValueClass(Text.class);

        job2.setMapperClass(TopKMapper.class);
        job2.setReducerClass(TopKReducer.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        job2.waitForCompletion(true);


    }
}

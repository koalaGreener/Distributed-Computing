import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;


public class BigramRelativeFrequency {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{

        final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String wordA;
            String wordB;

            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);

            if (itr.hasMoreTokens()){
                wordA = itr.nextToken();
                while (itr.hasMoreTokens()) {
                    wordB = itr.nextToken();
                    context.write(new Text(wordA + " " + wordB), one);
                    context.write(new Text(wordA + " " + "*"), one);
                    wordA = wordB;
                }

            }
        }
    }

    protected static class MyCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            context.write(key, result);

        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
        static final FloatWritable theresult = new FloatWritable();
        float marginal = 0.0f;
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }

            if (key.toString().contains("*")){
                theresult.set(1.0f * (float)sum);
                context.write(key, theresult);
                marginal = (float) sum;
            }
            else {
                theresult.set(1.0f * (float) sum / marginal);
                context.write(key, theresult);
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "BigramRelativeFrequency");
        job.setJarByClass(BigramRelativeFrequency.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(MyCombiner.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}

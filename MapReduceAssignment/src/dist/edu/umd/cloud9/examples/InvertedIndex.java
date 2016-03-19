package edu.umd.cloud9.examples;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfWritables;
import edu.umd.cloud9.util.EntryObject2IntFrequencyDistribution;
import edu.umd.cloud9.util.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.PairOfObjectInt;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;


public class InvertedIndex extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
        private static final Text word = new Text();
        private static final Object2IntFrequencyDistribution<String> counts = new EntryObject2IntFrequencyDistribution<String>();
        public void map(LongWritable docno, Text doc, Context context) throws IOException, InterruptedException {
            String text = doc.toString();
            counts.clear();
            String[] terms = text.split("\\s+");
            // First build a histogram of the terms.
            for (String term : terms) {
                if (term == null || term.length() == 0)
                {
                    continue;
                }
                counts.increment(term);
            }
            // Emit postings.
            for (PairOfObjectInt<String> e : counts) {
                word.set(e.getLeftElement());
                context.write(word, new PairOfInts((int) docno.get(), e.getRightElement()));
            }
        }
    }
    private static class MyReducer extends Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
        private final static IntWritable DF = new IntWritable();
        public void reduce(Text key, Iterable<PairOfInts> values, Context context) throws IOException, InterruptedException {
            Iterator<PairOfInts> iter = values.iterator();
            ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
            int df = 0;
            while (iter.hasNext())
            {
                postings.add(iter.next().clone());
                df++;
            }
            // Sort the postings by docno ascending.
            Collections.sort(postings);
            DF.set(df);
            context.write(key, new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job = new Job(conf1);
        job.setJobName("InvertedIndex");
        job.setJarByClass(InvertedIndex.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        // Delete the output directory if it exists already.
        Path outputDir = new Path(args[1]);
        FileSystem.get(getConf()).delete(outputDir, true);
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InvertedIndex(), args);
    }
}
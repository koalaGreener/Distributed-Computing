import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;


public class InvertedIndex {



    private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
        private static final Text WORD = new Text();
        private static final Object2IntFrequencyDistribution<String> COUNTS = new Object2IntFrequencyDistributionEntry<String>();

        public void map(LongWritable docno, Text doc, Context context) throws IOException, InterruptedException {
            String text = doc.toString();
            COUNTS.clear();
            String[] terms = text.split("\\s+");
// First build a histogram of the terms.
            for (String term : terms)
            {
                if (term == null || term.length() == 0) {
                    continue;
                }
                COUNTS.increment(term);
            }
// Emit postings.
            for (PairOfObjectInt<String> e : COUNTS) {
                WORD.set(e.getLeftElement());
                context.write(WORD, new PairOfInts((int) docno.get(), e.getRightElement()));
            }
        }
    }





    private static class MyReducer extends
            Reducer<Text, PairOfInts, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
        private final static IntWritable DF = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<PairOfInts> values, Context context)
                throws IOException, InterruptedException {
            Iterator<PairOfInts> iter = values.iterator();
            ArrayListWritable<PairOfInts> postings = new ArrayListWritable<PairOfInts>();
            int df = 0;
            while (iter.hasNext()) {
                postings.add(iter.next().clone());
                df++;
            }
// Sort the postings by docno ascending.
            Collections.sort(postings);
            DF.set(df);
            context.write(key,
                    new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, postings));
        }
    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "BigramRelativeFrequency");
        job.setJarByClass(BigramRelativeFrequency.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(1);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);



    }

}

package edu.umd.cloud9.examples;

import edu.umd.cloud9.io.ArrayListWritable;
import edu.umd.cloud9.io.PairOfInts;
import edu.umd.cloud9.io.PairOfWritables;
import edu.umd.cloud9.util.EntryObject2IntFrequencyDistribution;
import edu.umd.cloud9.util.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.PairOfObjectInt;
import org.apache.commons.cli.*;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;


public class InvertedIndex extends Configured implements Tool {
    private static class MyMapper extends Mapper<LongWritable, Text, Text, PairOfInts> {
        private static final Text WORD = new Text();
        private static final Object2IntFrequencyDistribution<String> COUNTS = new EntryObject2IntFrequencyDistribution<String>();
        @Override
        public void map(LongWritable docno, Text doc, Context context)
                throws IOException, InterruptedException {
            String text = doc.toString();
            COUNTS.clear();
            String[] terms = text.split("\\s+");
// First build a histogram of the terms.
            for (String term : terms) {
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
    private InvertedIndex() {}
    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));
        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();
        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }
        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
                Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

        Configuration conf1 = new Configuration();
        Job job = new Job(conf1);
        job.setJobName(InvertedIndex.class.getSimpleName());
        job.setJarByClass(InvertedIndex.class);
        job.setNumReduceTasks(reduceTasks);
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PairOfInts.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PairOfWritables.class);
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);
// Delete the output directory if it exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(getConf()).delete(outputDir, true);
        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        return 0;
    }
    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new InvertedIndex(), args);
    }
}
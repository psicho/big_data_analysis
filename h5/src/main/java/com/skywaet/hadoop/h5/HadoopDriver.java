package com.skywaet.hadoop.h5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Arrays;

public class HadoopDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new HadoopDriver(), args);
        System.exit(ret);
    }

    public int run(String[] args) throws Exception {

        if (args.length != 2) {
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path unsortedOutputPath = outputPath.getParent().suffix("/unsorted");

        Job mainJob = Job.getInstance(getConf());
        mainJob.setJarByClass(HadoopDriver.class);
        mainJob.setJobName("WordCounter");
        FileInputFormat.addInputPath(mainJob, inputPath);
        FileOutputFormat.setOutputPath(mainJob, unsortedOutputPath);

        mainJob.setMapperClass(WordMapper.class);
        mainJob.setReducerClass(Summer.class);
        mainJob.setMapOutputKeyClass(Text.class);
        mainJob.setMapOutputValueClass(Text.class);

        mainJob.setOutputKeyClass(Text.class);
        mainJob.setOutputValueClass(WordCountResult.class);

        mainJob.setOutputFormatClass(SequenceFileOutputFormat.class);



        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(mainJob)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(mainJob));

        int code = mainJob.waitForCompletion(true) ? 0 : 1;

        if (code == 0) {
            Job orderingJob = Job.getInstance(getConf());
            orderingJob.setInputFormatClass(SequenceFileInputFormat.class);

            orderingJob.setJarByClass(HadoopDriver.class);
            orderingJob.setJobName("Sorting");
            SequenceFileInputFormat.addInputPath(orderingJob, unsortedOutputPath);
            FileOutputFormat.setOutputPath(orderingJob, outputPath);

            orderingJob.setMapperClass(SortMapper.class);
            orderingJob.setMapOutputKeyClass(WordCountResult.class);
            orderingJob.setMapOutputValueClass(NullWritable.class);

            orderingJob.setReducerClass(SortReducer.class);

            Path totalOrderPartitioner = outputPath.getParent().suffix("/partitioner");

            TotalOrderPartitioner.setPartitionFile(orderingJob.getConfiguration(), totalOrderPartitioner);
            InputSampler.Sampler<WordCountResult, NullWritable> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
            InputSampler.writePartitionFile(orderingJob, sampler);
            orderingJob.setPartitionerClass(TotalOrderPartitioner.class);

            int orderingResult = orderingJob.waitForCompletion(true) ? 0 : 2;

            FileSystem.get(getConf()).delete(totalOrderPartitioner, true);
            FileSystem.get(getConf()).delete(unsortedOutputPath, true);

            return orderingResult;
        }
        return code;
    }

    public static class SortMapper extends Mapper<Object, WordCountResult, WordCountResult, NullWritable> {
        @Override
        protected void map(Object key, WordCountResult value, Mapper<Object, WordCountResult, WordCountResult, NullWritable>.Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class SortReducer extends Reducer<WordCountResult, NullWritable, Text, Text> {

        @Override
        protected void reduce(WordCountResult key, Iterable<NullWritable> values, Reducer<WordCountResult, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            context.write(new Text(key.getWord()), new Text(key.getFollowedBy() + " " + key.getCount()));
        }
    }
}

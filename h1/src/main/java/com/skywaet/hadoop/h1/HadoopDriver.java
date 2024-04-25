package com.skywaet.hadoop.h1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

    Job job = Job.getInstance(getConf());
    job.setJarByClass(HadoopDriver.class);
    job.setJobName("WordCounter");
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setMapperClass(WordMapper.class);
    job.setReducerClass(Summer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
    System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

    return job.waitForCompletion(true) ? 0 : 1;
  }
}

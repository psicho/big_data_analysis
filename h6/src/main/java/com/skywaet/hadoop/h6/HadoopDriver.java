package com.skywaet.hadoop.h6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

        if (args.length < 2) {
            ToolRunner.printGenericCommandUsage(System.err);
            System.err.println("USAGE: hadoop jar ... <input-dir> <output-dir>");
            System.exit(1);
        }

        Job job = Job.getInstance(getConf());
        job.setJarByClass(HadoopDriver.class);
        job.setJobName("Spectrum");
        System.out.println("Input dirs: " + new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(SpectrumMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setInputFormatClass(SpectrumInputFormat.class);

        System.out.println("Input dirs: " + Arrays.toString(FileInputFormat.getInputPaths(job)));
        System.out.println("Output dir: " + FileOutputFormat.getOutputPath(job));

        long t0 = System.currentTimeMillis();
        int ret = job.waitForCompletion(true) ? 0 : 1;
        long t1 = System.currentTimeMillis();
        double dt = (t1 - t0) * 1e-3;
        System.out.println("Time: " + dt + "s");
        return ret;
    }
}

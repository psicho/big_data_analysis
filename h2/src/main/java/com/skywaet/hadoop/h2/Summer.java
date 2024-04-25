package com.skywaet.hadoop.h2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Summer extends Reducer<IntWritable, LongWritable, IntWritable, LongWritable> {
  public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    long sum = 0;
    for (LongWritable val : values) {
      sum += val.get();
    }
    context.write(key, new LongWritable(sum));
  }
}

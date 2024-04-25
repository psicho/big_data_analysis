package com.skywaet.hadoop.h6;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SpectrumMapper extends Mapper<Text, RawSpectrum, Text, DoubleWritable> {

  public void map(Text key, RawSpectrum value, Context context)
      throws IOException, InterruptedException {
      context.write(key, new DoubleWritable(value.variance()));
  }

}

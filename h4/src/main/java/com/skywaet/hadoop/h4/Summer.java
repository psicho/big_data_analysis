package com.skywaet.hadoop.h4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Summer extends Reducer<Text, Text, Text, CountableText> {
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<Text, Integer> numOccurrences = new HashMap<>();
        for (Text value : values) {
            numOccurrences.put(value, numOccurrences.getOrDefault(value, 0) + 1);
        }
        CountableText mostPopularValue = numOccurrences.entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(it -> new CountableText(it.getKey().toString(), it.getValue()))
                .orElse(new CountableText("<NOT FOLLOWED BY ANY WORD>", 0));

        context.write(key, mostPopularValue);
    }
}

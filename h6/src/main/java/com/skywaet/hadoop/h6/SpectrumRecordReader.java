package com.skywaet.hadoop.h6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

public class SpectrumRecordReader extends RecordReader<Text, RawSpectrum> {

    private Text key;
    private RawSpectrum value = new RawSpectrum();
    private long start = 0;
    private long end = 0;
    private long pos = 0;
    private final Map<String, float[]> is = new HashMap<String, float[]>();
    private final Map<String, float[]> js = new HashMap<String, float[]>();
    private final Map<String, float[]> ks = new HashMap<String, float[]>();
    private final Map<String, float[]> ws = new HashMap<String, float[]>();
    private final Map<String, float[]> ds = new HashMap<String, float[]>();
    Set<String> keyss;
    Iterator<String> iter;

    // line example: 2012 01 01 00 00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   0.00   ...
    private static final Pattern LINE_PATTERN =
            Pattern.compile("([0-9]{4} [0-9]{2} [0-9]{2} [0-9]{2} [0-9]{2})(.*)");

    @Override
    public void close() throws IOException {
    }

    @Override
    public Text getCurrentKey() {
        return key;
    }

    @Override
    public RawSpectrum getCurrentValue() throws IOException {

        String t = key.toString();

        value.setField2("i", is.get(t));
        value.setField2("j", js.get(t));
        value.setField2("k", ks.get(t));
        value.setField2("w", ws.get(t));
        value.setField2("d", ds.get(t));

        return value;
    }

    @Override
    public float getProgress() {

        if (start == end)
            return 0.0f;
        else
            return Math.min(1.0f, (pos - start) / (float) (end - start));

    }

    @Override
    public void initialize(InputSplit generic_split, TaskAttemptContext context) throws IOException {

        CombineFileSplit split = (CombineFileSplit) generic_split;
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        for (Path path : split.getPaths()) {
            FSDataInputStream filein = fs.open(path);
            GZIPInputStream gzis = new GZIPInputStream(filein);
            LineReader in = new LineReader(gzis, conf);
            int off;
            String res;
            do {
                Text txt = new Text();
                off = in.readLine(txt);
                res = txt.toString().replaceAll("\\s+", " ").trim();
                Matcher matcher = LINE_PATTERN.matcher(res);

                while (!matcher.find()) {
                    off = in.readLine(txt);
                    res = txt.toString().replaceAll("\\s+", " ").trim();
                    if (off == 0) break;
                    matcher = LINE_PATTERN.matcher(res);
                }

                if (off == 0) break;
                res = res.trim();
                String key = res.substring(0, 16);
                String v = res.substring(16);
                List<String> valuesstr = new ArrayList<>();
                Matcher m = Pattern.compile("\\S+").matcher(v);

                while (m.find()) {
                    valuesstr.add(m.group());
                }

                float[] values = new float[valuesstr.size()];

                for (int i = 0; i < valuesstr.size(); i++) {
                    values[i] = Float.parseFloat(valuesstr.get(i));
                }

                String name = path.getName();
                if (name.contains("i")) is.put(key, values);
                if (name.contains("j")) js.put(key, values);
                if (name.contains("k")) ks.put(key, values);
                if (name.contains("w")) ws.put(key, values);
                if (name.contains("d")) ds.put(key, values);

            } while (off > 0);

            keyss = is.keySet();
            keyss.removeIf(p -> !js.containsKey(p));
            keyss.removeIf(p -> !ks.containsKey(p));
            keyss.removeIf(p -> !ws.containsKey(p));
            keyss.removeIf(p -> !ds.containsKey(p));
            iter = keyss.iterator();
        }

        this.pos = start;
    }

    @Override
    public boolean nextKeyValue() {
        if (iter.hasNext()) {
            key = new Text(iter.next());
            return true;
        } else
            return false;
    }

}

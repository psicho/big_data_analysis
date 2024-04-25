package com.skywaet.hadoop.h6;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SpectrumInputFormat extends InputFormat<Text, RawSpectrum> {

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        Map<String, List<FileStatus>> stationData = readStationData(context);
        return generateSplits(stationData, context);

    }

    @Override
    public RecordReader<Text, RawSpectrum> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        SpectrumRecordReader reader = new SpectrumRecordReader();
        reader.initialize(split, context);
        return reader;

    }

    private Map<String, List<FileStatus>> readStationData(JobContext context) throws IOException {
        Configuration conf = context.getConfiguration();

        Path inp = FileInputFormat.getInputPaths(context)[0];
        FileSystem fs = FileSystem.get(conf);

        FileStatus[] files = fs.listStatus(inp);
        Map<String, List<FileStatus>> stations = new HashMap<>();

        for (FileStatus file : files) {

            Path path = file.getPath();
            String filename = path.getName();
            String station = filename.substring(0, 5);

            List<FileStatus> stationFiles = stations.getOrDefault(station, new ArrayList<>());
            stationFiles.add(file);
            stations.put(station, stationFiles);

        }

        return stations;

    }

    private List<InputSplit> generateSplits(Map<String, List<FileStatus>> stations, JobContext context)
            throws IOException {
        List<InputSplit> res = new ArrayList<>();

        for (String station : stations.keySet()) {

            List<FileStatus> filesList = stations.get(station);
            Path[] filesPath = new Path[filesList.size()];
            long[] lengths = new long[filesList.size()];

            for (int i = 0; i < filesList.size(); i++) {
                filesPath[i] = filesList.get(i).getPath();
                lengths[i] = filesList.get(i).getLen();
            }

            res.add(new CombineFileSplit(filesPath, lengths));

        }

        return res;
    }

}

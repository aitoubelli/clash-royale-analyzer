package com.ple2025.nodes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Pass1Job {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Pass1Job <input> <output>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Pass1: Archetype Frequencies");
        job.setJarByClass(Pass1Job.class);
        job.setMapperClass(Pass1Mapper.class);
        job.setCombinerClass(Pass1Reducer.class); // ← ADD THIS LINE
        job.setReducerClass(Pass1Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

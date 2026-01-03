package com.ple2025.stats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class StatJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: StatJob <edges_input> <nodes_input> <N_all_value> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        conf.setLong("N_ALL", Long.parseLong(args[2]));

        Job job = Job.getInstance(conf, "Part III: Stats");
        job.setJarByClass(StatJob.class);
        job.setMapperClass(StatMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Add nodes to DistributedCache
        job.addCacheFile(new Path(args[1]).toUri());

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

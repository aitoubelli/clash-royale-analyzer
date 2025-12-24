package com.ple2025.cleaning;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataCleaningJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: DataCleaningJob <input> <output> <mode>");
            System.err.println("  mode: 'clean' or 'duplicates'");
            System.exit(1);
        }

        String input = args[0];
        String output = args[1];
        String mode = args[2];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clash Royale Data Cleaning");

        job.setJarByClass(DataCleaningJob.class);
        job.setMapperClass(CleanMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        if ("clean".equals(mode)) {
            job.setReducerClass(DedupReducer.class);
        } else if ("duplicates".equals(mode)) {
            job.setReducerClass(DuplicateOnlyReducer.class);
        } else {
            throw new IllegalArgumentException("Mode must be 'clean' or 'duplicates'");
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(input));
        TextOutputFormat.setOutputPath(job, new Path(output));

        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}

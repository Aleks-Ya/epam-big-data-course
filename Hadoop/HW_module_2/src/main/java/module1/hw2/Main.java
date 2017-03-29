package module1.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("Start");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");

        Job job = Job.getInstance(conf, "The longest word");
        job.setJarByClass(Main.class);
        ChainMapper.addMapper(job, StringToWordMapper.class, NullWritable.class, Text.class, Text.class, NullWritable.class, conf);
        ChainMapper.addMapper(job, LongestWordCombiner.class, Text.class, NullWritable.class, Text.class, NullWritable.class, conf);
        job.setReducerClass(LongestWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path inputFile = new Path(args[0]);
        log.info("Input file: " + inputFile);
        FileInputFormat.addInputPath(job, inputFile);

        FileSystem fs = FileSystem.get(conf);
        Path outputFile = new Path(args[1]);
        log.info("Output file: " + outputFile);
        if (fs.exists(outputFile)) {
            log.info("Output file exists. Delete it: " + fs.delete(outputFile, true));
        }
        FileOutputFormat.setOutputPath(job, outputFile);

        log.info("Run job");
        boolean success = job.waitForCompletion(true);
        log.info("Job finished");

        log.info("Exit");
        System.exit(success ? 0 : 1);
    }
}

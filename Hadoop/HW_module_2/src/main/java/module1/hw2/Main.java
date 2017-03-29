package module1.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        log.info("Start");
        Configuration conf = makeConfig();
        Job job = makeJob(conf);
        configureInputFile(job, args[0]);
        FileSystem fs = FileSystem.get(conf);
        Path outputDir = configureOutputDir(job, args[1], fs);
        executeJob(job);
        printResultToConsole(fs, outputDir);
        log.info("Exit");
    }

    private static void printResultToConsole(FileSystem fs, Path outputDir) throws IOException {
        FileStatus[] outputFiles = fs.listStatus(outputDir);
        StringBuilder longestWord = new StringBuilder();
        for (FileStatus outputFile : outputFiles) {
            try (FSDataInputStream resultIS = fs.open(outputFile.getPath())) {
                String content = new BufferedReader(new InputStreamReader(resultIS)).lines().collect(Collectors.joining());
                longestWord.append(content);
            }

        }
        log.info("THE LONGEST WORD: " + longestWord);
    }

    private static void executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        log.info("Run job");
        boolean success = job.waitForCompletion(true);
        log.info("Job finished");
        if (!success) {
            throw new RuntimeException("Job filed");
        }
    }

    private static Path configureOutputDir(Job job, String arg, FileSystem fs) throws IOException {
        Path outputDir = new Path(arg);
        log.info("Output dir: " + outputDir);
        if (fs.exists(outputDir)) {
            log.info("Output dir exists. Delete it: " + fs.delete(outputDir, true));
        }
        FileOutputFormat.setOutputPath(job, outputDir);
        return outputDir;
    }

    private static void configureInputFile(Job job, String arg) throws IOException {
        Path inputFile = new Path(arg);
        log.info("Input file: " + inputFile);
        FileInputFormat.addInputPath(job, inputFile);
    }

    private static Job makeJob(Configuration conf) throws IOException {
        Job job = Job.getInstance(conf, "The longest word");
        job.setJarByClass(Main.class);
        ChainMapper.addMapper(job, StringToWordMapper.class, NullWritable.class, Text.class, Text.class, NullWritable.class, conf);
        ChainMapper.addMapper(job, LongestWordCombiner.class, Text.class, NullWritable.class, Text.class, NullWritable.class, conf);
        job.setReducerClass(LongestWordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job;
    }

    private static Configuration makeConfig() {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        return conf;
    }
}

package Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Task3.WordSentimentScoreMapper;
import Task3.WordSentimentScoreReducer;

/**
 * Driver for the Sentiment Scoring MapReduce job.
 * 
 * This job:
 * 1. Takes output from Task 1 or Task 2
 * 2. Assigns sentiment scores to texts by matching words to a sentiment lexicon
 * 3. Aggregates scores for each book and year
 */
public class WordSentimentScoreDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: WordSentimentScoreDriver <input path> <output path> [lexicon path]");
            System.err.println("Example: WordSentimentScoreDriver /output/part-r-00000 /output-sentiment [hdfs:///lexicons/afinn.txt]");
            return -1;
        }

        // Create and configure a new job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Sentiment Scoring");
        
        // Set the driver class
        job.setJarByClass(WordSentimentScoreDriver.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(WordSentimentScoreMapper.class);
        job.setReducerClass(WordSentimentScoreReducer.class);
        
        // Set map output key and value classes
        job.setMapOutputKeyClass(WordSentimentScoreMapper.BookKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        // Add lexicon file to distributed cache if provided
        if (args.length == 3) {
            job.addCacheFile(new Path(args[2]).toUri());
        }
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        // Check if output directory exists and delete it if it does
        Path outputPath = new Path(args[1]);
        FileSystem fs = outputPath.getFileSystem(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output directory " + outputPath + " deleted.");
        }
        
        FileOutputFormat.setOutputPath(job, outputPath);
        
        // Submit the job and wait for completion
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordSentimentScoreDriver(), args);
        System.exit(exitCode);
    }
}
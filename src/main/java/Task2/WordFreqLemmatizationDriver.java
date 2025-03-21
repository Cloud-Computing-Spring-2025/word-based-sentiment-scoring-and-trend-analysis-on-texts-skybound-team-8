package Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import Task2.WordFreqLemmatizationMapper;
import Task2.WordFreqLemmatizationReducer;

/**
 * Driver for the Lemma Frequency Analysis MapReduce job.
 * 
 * This job:
 * 1. Takes the cleaned dataset from Task 1 (output/part-r-00000)
 * 2. Performs sentence splitting and lemmatization
 * 3. Computes frequency of each lemma per book and year
 * 
 * Command: hadoop jar yourjar.jar Task2.driver.WordFreqLemmatizationDriver output/part-r-00000 output-lemma
 */
public class WordFreqLemmatizationDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
       if (args.length != 3) {
            System.err.println("Usage: WordFreqLemmatizationDriver <input path> <output path>");
            System.err.println("Example: WordFreqLemmatizationDriver output/part-r-00000 output-lemma");
            return -1;
        }


        // Create and configure a new job
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Lemma Frequency Analysis");
        
        // Set the driver class
        job.setJarByClass(WordFreqLemmatizationDriver.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(WordFreqLemmatizationMapper.class);
        job.setReducerClass(WordFreqLemmatizationReducer.class);
        
        // Set map output key and value classes
        job.setMapOutputKeyClass(WordFreqLemmatizationMapper.LemmaKey.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[1]));
        
        // Check if output directory exists and delete it if it does
        Path outputPath = new Path(args[2]);
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
        int exitCode = ToolRunner.run(new WordFreqLemmatizationDriver(), args);
        System.exit(exitCode);
    }
}

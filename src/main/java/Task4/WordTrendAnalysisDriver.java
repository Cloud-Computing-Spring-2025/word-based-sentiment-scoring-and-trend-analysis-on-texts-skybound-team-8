package Task4;

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

import Task4.WordTrendAnalysisMapper;
import Task4.WordTrendAnalysisReducer;

/**
 * Driver for the Trend Analysis and Aggregation MapReduce job.
 * 
 * This job:
 * 1. Takes sentiment scores and word frequency data from previous tasks
 * 2. Aggregates data by decade, with options for book-level and overall analysis
 * 3. Produces a consolidated dataset of trends over time
 */
public class WordTrendAnalysisDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: TrendAnalysisDriver <input path> <output path> [options]");
            System.err.println("Options:");
            System.err.println("  -book=true|false     Include book-level trends (default: true)");
            System.err.println("  -overall=true|false  Include overall decade trends (default: true)");
            System.err.println("  -average=true|false  Use average instead of sum for aggregation (default: true)");
            System.err.println("Example: TrendAnalysisDriver /output-sentiment /output-trends -book=true -overall=true -average=true");
            return -1;
        }

        // Create and configure a new job
        Configuration conf = getConf();
        
        // Set default options
        boolean includeBookLevel = true;
        boolean includeOverallLevel = true;
        boolean useAverage = true;
        
        // Parse optional arguments
        for (int i = 2; i < args.length; i++) {
            String arg = args[i];
            
            if (arg.startsWith("-book=")) {
                includeBookLevel = Boolean.parseBoolean(arg.substring(6));
            } else if (arg.startsWith("-overall=")) {
                includeOverallLevel = Boolean.parseBoolean(arg.substring(9));
            } else if (arg.startsWith("-average=")) {
                useAverage = Boolean.parseBoolean(arg.substring(9));
            }
        }
        
        // Set options in configuration
        conf.setBoolean("trend.include.book", includeBookLevel);
        conf.setBoolean("trend.include.overall", includeOverallLevel);
        conf.setBoolean("trend.use.average", useAverage);
        
        Job job = Job.getInstance(conf, "Trend Analysis");
        
        // Set the driver class
        job.setJarByClass(WordTrendAnalysisDriver.class);
        
        // Set mapper and reducer classes
        job.setMapperClass(WordTrendAnalysisMapper.class);
        job.setReducerClass(WordTrendAnalysisReducer.class);
        
        // Set map output key and value classes
        job.setMapOutputKeyClass(WordTrendAnalysisMapper.TrendKey.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        
        // Set final output key and value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
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
        int exitCode = ToolRunner.run(new WordTrendAnalysisDriver(), args);
        System.exit(exitCode);
    }
}
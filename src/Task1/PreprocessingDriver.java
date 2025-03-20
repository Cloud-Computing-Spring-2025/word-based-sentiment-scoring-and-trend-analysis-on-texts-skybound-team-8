
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PreprocessingDriver {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: PreprocessingDriver <input directory> <output directory>");
            System.exit(-1);
        }

        // Create a new Hadoop job
        Job job = Job.getInstance();
        job.setJarByClass(PreprocessingDriver.class);
        job.setJobName("Preprocessing Job");

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0])); // Input directory
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output directory

        // Set Mapper and Reducer classes
        job.setMapperClass(PreprocessingMapper.class);
        job.setReducerClass(PreprocessingReducer.class);

        // Set output key and value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
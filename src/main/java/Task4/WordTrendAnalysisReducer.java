package Task4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for trend analysis and aggregation.
 * 
 * Input: Key-value pairs where key is either (bookID, decade) or decade,
 *        and value is sentiment score or word frequency
 * Output: Consolidated dataset summarizing trends per decade, with option
 *         to break down by individual books
 */
public class WordTrendAnalysisReducer extends Reducer<WordTrendAnalysisMapper.TrendKey, DoubleWritable, Text, Text> {
    
    private boolean useAverage = true;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        // Get configuration for aggregation method (sum or average)
        useAverage = conf.getBoolean("trend.use.average", true);
    }
    
    @Override
    public void reduce(WordTrendAnalysisMapper.TrendKey key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double sum = 0.0;
        int count = 0;
        double min = Double.MAX_VALUE;
        double max = Double.MIN_VALUE;
        
        // Sum up all values and track statistics
        for (DoubleWritable val : values) {
            double value = val.get();
            sum += value;
            count++;
            
            // Track min and max
            if (value < min) min = value;
            if (value > max) max = value;
        }
        
        // Calculate final score based on configuration
        double finalScore = useAverage ? (count > 0 ? sum / count : 0.0) : sum;
        
        // Prepare output key
        Text outputKey = new Text(key.toString());
        
        // Prepare output value with additional statistics
        StringBuilder outputValueBuilder = new StringBuilder();
        outputValueBuilder.append(String.format("%.2f", finalScore));
        outputValueBuilder.append("\t").append(count); // Add count of data points
        
        // Include min and max if we have values
        if (count > 0) {
            outputValueBuilder.append("\t").append(String.format("%.2f", min));
            outputValueBuilder.append("\t").append(String.format("%.2f", max));
        }
        
        Text outputValue = new Text(outputValueBuilder.toString());
        
        // Emit final result
        context.write(outputKey, outputValue);
    }
}
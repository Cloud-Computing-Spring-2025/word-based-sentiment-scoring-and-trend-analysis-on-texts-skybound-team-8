package Task3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for aggregating sentiment scores for books.
 * 
 * Input: Key-value pairs where key is (bookID, year) and values are sentiment scores
 * Output: Each book (and its year) mapped to a cumulative sentiment score
 */
public class WordSentimentScoreReducer extends Reducer<WordSentimentScoreMapper.BookKey, DoubleWritable, Text, DoubleWritable> {
    
    @Override
    public void reduce(WordSentimentScoreMapper.BookKey key, Iterable<DoubleWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double totalScore = 0.0;
        int count = 0;
        
        // Sum up all sentiment scores for this book and year
        for (DoubleWritable val : values) {
            totalScore += val.get();
            count++;
        }
        
        // Normalize the score by the number of sentiment words (optional)
        // Uncomment to use normalized score instead of total
        // double normalizedScore = count > 0 ? totalScore / count : 0.0;
        
        // Output the book key and its sentiment score
        Text outputKey = new Text(key.toString());
        DoubleWritable outputValue = new DoubleWritable(totalScore);
        
        context.write(outputKey, outputValue);
    }
}
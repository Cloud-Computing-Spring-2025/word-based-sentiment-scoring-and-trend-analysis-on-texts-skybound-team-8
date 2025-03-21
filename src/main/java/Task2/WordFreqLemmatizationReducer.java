package Task2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer for word frequency analysis with lemmatization.
 * 
 * Input: Key-value pairs with key as (bookID, lemma, year) and values as counts (1s)
 * Output: Produces a dataset listing each lemma with its frequency along with book ID and year
 */
public class WordFreqLemmatizationReducer extends Reducer<WordFreqLemmatizationMapper.LemmaKey, IntWritable, Text, Text> {
    
    @Override
    public void reduce(WordFreqLemmatizationMapper.LemmaKey key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        // Sum up all the counts for this lemma
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        
        // Format the output key as the book ID
        Text outputKey = new Text(key.getBookId());
        
        // Format the output value as: lemma, year, frequency
        StringBuilder outputValueBuilder = new StringBuilder();
        outputValueBuilder.append(key.getLemma()).append("\t");
        outputValueBuilder.append(key.getYear()).append("\t");
        outputValueBuilder.append(sum);
        
        Text outputValue = new Text(outputValueBuilder.toString());
        
        // Emit the result
        context.write(outputKey, outputValue);
    }
}
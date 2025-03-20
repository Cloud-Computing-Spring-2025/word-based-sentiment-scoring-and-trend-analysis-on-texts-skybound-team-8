
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        // Aggregate cleaned text for the same key
        StringBuilder aggregatedText = new StringBuilder();
        for (Text value : values) {
            System.out.println("Key: " + key + ", Value: " + value);
            aggregatedText.append(value.toString()).append(" ");
        }

        // Emit the composite key and aggregated cleaned text
        context.write(key, new Text(aggregatedText.toString().trim()));
    }
}
package Task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.HashSet;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    
    private static final HashSet<String> STOP_WORDS = new HashSet<>(
        Arrays.asList("the", "and", "is", "in", "to", "of", "that", "this", "it", "for", "on", "with", "as", "was", "at", "by", "an")
    );

    private Text bookID = new Text();
    private Text cleanedText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Extract file name to get book ID
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String bookId = fileName.split("_")[0]; // Extracts "PGXXX" from "PGXXX_raw.txt"
        bookID.set(bookId);

        // Convert to lowercase and remove non-alphabetic characters
        String line = value.toString().toLowerCase().replaceAll("[^a-z\\s]", "");
        StringTokenizer tokenizer = new StringTokenizer(line);
        StringBuilder processedText = new StringBuilder();

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            if (!STOP_WORDS.contains(word)) {
                processedText.append(word).append(" ");
            }
        }

        if (processedText.length() > 0) {
            cleanedText.set(processedText.toString().trim());
            context.write(bookID, cleanedText);
        }
    }
}

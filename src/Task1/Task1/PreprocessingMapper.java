// 

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

    private Text compositeKey = new Text();
    private Text cleanedText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        String[] fileParts = fileName.split("_"); // Expecting format "PGXXX_Title_Year.txt"

        if (fileParts.length < 3) return; // Ensure the filename has enough parts

        String bookId = fileParts[0];  // Extract Book ID (PGXXX)
        String bookTitle = fileParts[1].replace("-", " "); // Replace hyphens with spaces
        String year = fileParts[2].replaceAll("[^0-9]", ""); // Extract numeric year

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
            compositeKey.set(bookId + "," + bookTitle + "," + year);
            cleanedText.set(processedText.toString().trim());
            context.write(compositeKey, cleanedText);
        }
    }
}


package Task3;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for sentiment scoring of books.
 * 
 * Input: Output from Task 1 or Task 2
 * Output: Key-value pairs where key is (bookID, year) and value is sentiment score
 */
public class WordSentimentScoreMapper extends Mapper<Object, Text, WordSentimentScoreMapper.BookKey, DoubleWritable> {
    
    /**
     * Custom composite key class for book identification by ID and year
     */
    public static class BookKey implements WritableComparable<BookKey> {
        private String bookId;
        private int year;
        
        // Default constructor required for Hadoop serialization
        public BookKey() {
        }
        
        public BookKey(String bookId, int year) {
            this.bookId = bookId;
            this.year = year;
        }
        
        @Override
        public void write(java.io.DataOutput out) throws IOException {
            WritableUtils.writeString(out, bookId);
            out.writeInt(year);
        }
        
        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            bookId = WritableUtils.readString(in);
            year = in.readInt();
        }
        
        @Override
        public int compareTo(BookKey other) {
            int cmp = this.bookId.compareTo(other.bookId);
            if (cmp != 0) {
                return cmp;
            }
            return Integer.compare(this.year, other.year);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof BookKey) {
                BookKey other = (BookKey) obj;
                return this.bookId.equals(other.bookId) && this.year == other.year;
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return bookId.hashCode() * 163 + year;
        }
        
        @Override
        public String toString() {
            return "(" + bookId + "," + year + ")";
        }
        
        // Getters
        public String getBookId() {
            return bookId;
        }
        
        public int getYear() {
            return year;
        }
    }
    
    private Map<String, Double> sentimentLexicon = new HashMap<>();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        // Load the sentiment lexicon from distributed cache
        try {
            // The lexicon file should be added to the distributed cache
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI cacheFile : cacheFiles) {
                    if (cacheFile.getPath().endsWith("afinn.txt")) {
                        loadAfinnLexicon(cacheFile, conf);
                    }
                }
            } else {
                // If no cache files available, load a minimal built-in lexicon
                loadMinimalLexicon();
            }
        } catch (Exception e) {
            System.err.println("Error loading sentiment lexicon: " + e.getMessage());
            // Fall back to minimal lexicon
            loadMinimalLexicon();
        }
    }
    
    /**
     * Load the AFINN sentiment lexicon from a file in distributed cache
     */
    private void loadAfinnLexicon(URI lexiconFileUri, Configuration conf) throws IOException {
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(FileSystem.get(lexiconFileUri, conf).open(new Path(lexiconFileUri))))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // AFINN format: word\tscore
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String word = parts[0].trim().toLowerCase();
                    try {
                        double score = Double.parseDouble(parts[1].trim());
                        sentimentLexicon.put(word, score);
                    } catch (NumberFormatException e) {
                        System.err.println("Invalid score format for word: " + word);
                    }
                }
            }
        }
    }
    
    /**
     * Load a minimal built-in sentiment lexicon for testing
     */
    private void loadMinimalLexicon() {
        // Common positive words
        sentimentLexicon.put("good", 2.0);
        sentimentLexicon.put("great", 3.0);
        sentimentLexicon.put("excellent", 3.0);
        sentimentLexicon.put("happy", 2.0);
        sentimentLexicon.put("love", 3.0);
        sentimentLexicon.put("wonderful", 3.0);
        sentimentLexicon.put("joy", 2.0);
        sentimentLexicon.put("success", 2.0);
        sentimentLexicon.put("beautiful", 2.0);
        sentimentLexicon.put("best", 3.0);
        
        // Common negative words
        sentimentLexicon.put("bad", -2.0);
        sentimentLexicon.put("awful", -3.0);
        sentimentLexicon.put("terrible", -3.0);
        sentimentLexicon.put("sad", -2.0);
        sentimentLexicon.put("hate", -3.0);
        sentimentLexicon.put("poor", -2.0);
        sentimentLexicon.put("fail", -2.0);
        sentimentLexicon.put("failure", -2.0);
        sentimentLexicon.put("worst", -3.0);
        sentimentLexicon.put("angry", -2.0);
        
        // Neutral modifiers
        sentimentLexicon.put("very", 0.5);
        sentimentLexicon.put("not", -1.0);
        sentimentLexicon.put("no", -1.0);
    }
    
    /**
     * Get sentiment score for a word from the lexicon
     */
    private double getSentimentScore(String word) {
        word = word.toLowerCase();
        return sentimentLexicon.getOrDefault(word, 0.0);
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String bookId;
            int year;
            String text;
            
            // Process input based on expected format
            // Check if the input is from Task 1 in format: (bookId,year) cleanedText
            if (line.startsWith("(") && line.contains(")")) {
                int openBracketIndex = line.indexOf('(');
                int closeBracketIndex = line.indexOf(')');
                
                String keyPart = line.substring(openBracketIndex + 1, closeBracketIndex);
                String[] keyParts = keyPart.split(",");
                
                if (keyParts.length != 2) {
                    return; // Skip malformed keys
                }
                
                bookId = keyParts[0].trim();
                
                try {
                    year = Integer.parseInt(keyParts[1].trim());
                } catch (NumberFormatException e) {
                    return; // Skip records with invalid years
                }
                
                // Get text after closing bracket
                text = line.substring(closeBracketIndex + 1).trim();
            }
            // Check if input is from Task 2 in format: bookId lemma year frequency
            else if (line.contains("\t")) {
                String[] parts = line.split("\t");
                if (parts.length >= 3) {
                    bookId = parts[0].trim();
                    
                    try {
                        // The lemma is in parts[1], year in parts[2]
                        year = Integer.parseInt(parts[2].trim());
                        // For Task 2 output, we'll treat each lemma as the text
                        text = parts[1].trim();
                    } catch (NumberFormatException e) {
                        return; // Skip records with invalid years
                    }
                } else {
                    return; // Skip malformed records
                }
            } else {
                return; // Skip unrecognized format
            }
            
            // Create key for output
            BookKey outputKey = new BookKey(bookId, year);
            
            // Calculate sentiment scores for all words in the text
            double totalScore = 0.0;
            
            StringTokenizer tokenizer = new StringTokenizer(text);
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                double score = getSentimentScore(token);
                
                if (score != 0.0) {
                    // Emit individual token scores
                    context.write(outputKey, new DoubleWritable(score));
                    totalScore += score;
                }
            }
            
            // Also emit the total score for this text if any sentiment words were found
            if (totalScore != 0.0) {
                context.write(outputKey, new DoubleWritable(totalScore));
            }
            
        } catch (Exception e) {
            System.err.println("Error processing input: " + value);
            e.printStackTrace();
        }
    }
}
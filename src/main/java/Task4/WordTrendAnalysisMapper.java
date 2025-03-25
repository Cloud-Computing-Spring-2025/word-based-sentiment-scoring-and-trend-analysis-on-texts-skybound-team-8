package Task4;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper for trend analysis and aggregation.
 * 
 * Input: Sentiment scores and word frequency data from previous tasks
 * Output: Key-value pairs where the key is either (bookID, decade) or just decade,
 *         and the value is the sentiment score or word frequency
 */
public class WordTrendAnalysisMapper extends Mapper<Object, Text, WordTrendAnalysisMapper.TrendKey, DoubleWritable> {
    
    /**
     * Custom composite key class for trend analysis
     */
    public static class TrendKey implements WritableComparable<TrendKey> {
        private String bookId; // Optional, may be empty for overall decade trends
        private int decade;
        private boolean isOverall; // Flag to indicate if this is an overall decade trend
        
        // Default constructor required for Hadoop serialization
        public TrendKey() {
        }
        
        // Constructor for book-specific decade trends
        public TrendKey(String bookId, int decade) {
            this.bookId = bookId;
            this.decade = decade;
            this.isOverall = false;
        }
        
        // Constructor for overall decade trends
        public TrendKey(int decade) {
            this.bookId = "";
            this.decade = decade;
            this.isOverall = true;
        }
        
        @Override
        public void write(java.io.DataOutput out) throws IOException {
            WritableUtils.writeString(out, bookId);
            out.writeInt(decade);
            out.writeBoolean(isOverall);
        }
        
        @Override
        public void readFields(java.io.DataInput in) throws IOException {
            bookId = WritableUtils.readString(in);
            decade = in.readInt();
            isOverall = in.readBoolean();
        }
        
        @Override
        public int compareTo(TrendKey other) {
            // First compare by decade
            int cmp = Integer.compare(this.decade, other.decade);
            if (cmp != 0) {
                return cmp;
            }
            
            // Then compare by isOverall flag (overall trends come first)
            if (this.isOverall != other.isOverall) {
                return this.isOverall ? -1 : 1;
            }
            
            // Finally compare by bookId (only relevant if both are book-specific)
            if (!this.isOverall) {
                return this.bookId.compareTo(other.bookId);
            }
            
            return 0;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TrendKey) {
                TrendKey other = (TrendKey) obj;
                if (this.isOverall && other.isOverall) {
                    return this.decade == other.decade;
                } else if (!this.isOverall && !other.isOverall) {
                    return this.decade == other.decade && this.bookId.equals(other.bookId);
                }
                return false;
            }
            return false;
        }
        
        @Override
        public int hashCode() {
            return isOverall ? decade : (bookId.hashCode() * 163 + decade);
        }
        
        @Override
        public String toString() {
            return isOverall ? String.valueOf(decade) + "s" : "(" + bookId + "," + decade + "s)";
        }
        
        // Getters
        public String getBookId() {
            return bookId;
        }
        
        public int getDecade() {
            return decade;
        }
        
        public boolean isOverall() {
            return isOverall;
        }
    }
    
    private boolean includeBookLevel = true;
    private boolean includeOverallLevel = true;
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        
        // Get configuration for which levels to include
        includeBookLevel = conf.getBoolean("trend.include.book", true);
        includeOverallLevel = conf.getBoolean("trend.include.overall", true);
    }
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        try {
            String line = value.toString();
            String bookId;
            int year;
            double score;
            
            // Process input based on expected format
            if (line.contains("\t")) {
                // Format: (bookId,year) [tab] score 
                // OR: bookId [tab] lemma [tab] year [tab] frequency
                String[] parts = line.split("\t");
                
                if (parts.length < 2) {
                    return; // Skip malformed lines
                }
                
                // Check if this is sentiment score output
                if (parts[0].startsWith("(") && parts[0].endsWith(")")) {
                    // Format: (bookId,year) [tab] score
                    String keyPart = parts[0].substring(1, parts[0].length() - 1);
                    String[] keyParts = keyPart.split(",");
                    
                    if (keyParts.length != 2) {
                        return; // Skip malformed keys
                    }
                    
                    bookId = keyParts[0].trim();
                    
                    try {
                        year = Integer.parseInt(keyParts[1].trim());
                        score = Double.parseDouble(parts[1].trim());
                    } catch (NumberFormatException e) {
                        return; // Skip records with invalid numbers
                    }
                } 
                // Check if this is lemma frequency output
                else if (parts.length >= 4) {
                    // Format: bookId [tab] lemma [tab] year [tab] frequency
                    bookId = parts[0].trim();
                    
                    try {
                        year = Integer.parseInt(parts[2].trim());
                        score = Double.parseDouble(parts[3].trim()); // frequency as score
                    } catch (NumberFormatException e) {
                        return; // Skip records with invalid numbers
                    }
                } else {
                    return; // Skip unrecognized format
                }
            } else {
                return; // Skip unrecognized format
            }
            
            // Calculate the decade
            int decade = (year / 10) * 10;
            
            // Emit book-level trend if configured
            if (includeBookLevel) {
                TrendKey bookDecadeKey = new TrendKey(bookId, decade);
                context.write(bookDecadeKey, new DoubleWritable(score));
            }
            
            // Emit overall decade trend if configured
            if (includeOverallLevel) {
                TrendKey overallDecadeKey = new TrendKey(decade);
                context.write(overallDecadeKey, new DoubleWritable(score));
            }
            
        } catch (Exception e) {
            System.err.println("Error processing input: " + value);
            e.printStackTrace();
        }
    }
}
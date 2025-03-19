// import java.io.IOException;
// import java.util.HashSet;
// import java.util.Set;
// import org.apache.hadoop.io.LongWritable;
// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Mapper;

// public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
//     private Text compositeKey = new Text();
//     private Text cleanedText = new Text();

//     // Predefined list of stop words
//     private static final Set<String> STOP_WORDS = new HashSet<String>() {{
//         add("the"); add("and"); add("of"); add("to"); add("in"); add("a"); add("is"); add("it"); add("that"); add("with");
//     }};

//     // Variables to track metadata
//     private String title = null;
//     private String year = null;

//     @Override
//     public void map(LongWritable key, Text value, Context context) 
//             throws IOException, InterruptedException {
//         String line = value.toString();
//         System.out.println("Raw Line: " + line);  // Log the raw line

//         // Extract title if not already found
//         if (title == null && line.contains("Title:")) {
//             title = extractTitle(line);
//             System.out.println("Extracted Title: " + title);  // Log extracted title
//         }

//         // Extract year if not already found
//         if (year == null && line.contains("Release date:")) {
//             year = extractYear(line);
//             System.out.println("Extracted Year: " + year);  // Log extracted year
//         }

//         // If both title and year are found, emit the key-value pair
//         if (title != null && year != null) {
//             String cleanedLine = cleanText(line);
//             compositeKey.set(title + "," + year);
//             cleanedText.set(cleanedLine);
//             context.write(compositeKey, cleanedText);
//         } else {
//             System.err.println("Skipping line with no title or year: " + line);
//         }
//     }

//     private String extractTitle(String line) {
//         // Extract the title part after "Title:"
//         String titlePart = line.split("Title:")[1].trim();
//         // Remove any trailing metadata like "(1711)" or ", and excerpts..."
//         titlePart = titlePart.split("\\(")[0].trim();  // Remove "(1711)"
//         titlePart = titlePart.split(",")[0].trim();   // Remove ", and excerpts..."
//         return titlePart;
//     }

//     private String extractYear(String line) {
//         // Extract the year from the release date
//         String[] parts = line.split(" ");
//         for (String part : parts) {
//             if (part.matches("\\d{4}")) {  // Match 4-digit year like "2005"
//                 return part;
//             }
//         }
//         return null;
//     }

//     private String cleanText(String text) {
//         text = text.toLowerCase().replaceAll("[^a-z\\s]", "");
//         StringBuilder cleanedText = new StringBuilder();
//         for (String word : text.split("\\s+")) {
//             if (!STOP_WORDS.contains(word) && !word.isEmpty()) {
//                 cleanedText.append(word).append(" ");
//             }
//         }
//         return cleanedText.toString().trim();
//     }
// }

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PreprocessingMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text compositeKey = new Text();
    private Text cleanedText = new Text();

    // Predefined list of stop words
    private static final Set<String> STOP_WORDS = new HashSet<String>() {{
        add("the"); add("and"); add("of"); add("to"); add("in"); add("a"); add("is"); add("it"); add("that"); add("with");
    }};

    // Variables to track metadata
    private String title = null;
    private String year = null;
    private String bookId = null;

    @Override
    public void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        String line = value.toString();
        System.out.println("Raw Line: " + line);  // Log the raw line

        // Extract book ID if not already found
        if (bookId == null && line.contains("eBook #")) {
            bookId = extractBookId(line);
            System.out.println("Extracted Book ID: " + bookId);  // Log extracted Book ID
        }

        // Extract title if not already found
        if (title == null && line.contains("Title:")) {
            title = extractTitle(line);
            System.out.println("Extracted Title: " + title);  // Log extracted title
        }

        // Extract year if not already found
        if (year == null && line.contains("Release date:")) {
            year = extractYear(line);
            System.out.println("Extracted Year: " + year);  // Log extracted year
        }

        // If Book ID, Title, and Year are found, emit the key-value pair
        if (bookId != null && title != null && year != null) {
            String cleanedLine = cleanText(line);
            compositeKey.set(bookId + "," + title + "," + year);  // Composite key: Book ID, Title, Year
            cleanedText.set(cleanedLine);
            context.write(compositeKey, cleanedText);
        } else {
            System.err.println("Skipping line with no Book ID, Title or Year: " + line);
        }
    }

    private String extractBookId(String line) {
        // Extract Book ID from the line (e.g., "eBook #14800")
        String[] parts = line.split("eBook #");
        if (parts.length > 1) {
            return parts[1].split("\\s")[0];  // Extract the number after "eBook #"
        }
        return null;
    }

    private String extractTitle(String line) {
        // Extract the title part after "Title:"
        String titlePart = line.split("Title:")[1].trim();
        // Remove any trailing metadata like "(1711)" or ", and excerpts..."
        titlePart = titlePart.split("\\(")[0].trim();  // Remove "(1711)"
        titlePart = titlePart.split(",")[0].trim();   // Remove ", and excerpts..."
        return titlePart;
    }

    private String extractYear(String line) {
        // Extract the year from the release date
        String[] parts = line.split(" ");
        for (String part : parts) {
            if (part.matches("\\d{4}")) {  // Match 4-digit year like "2005"
                return part;
            }
        }
        return null;
    }

    private String cleanText(String text) {
        text = text.toLowerCase().replaceAll("[^a-z\\s]", "");
        StringBuilder cleanedText = new StringBuilder();
        for (String word : text.split("\\s+")) {
            if (!STOP_WORDS.contains(word) && !word.isEmpty()) {
                cleanedText.append(word).append(" ");
            }
        }
        return cleanedText.toString().trim();
    }
}

// package Task1;


// import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapreduce.Reducer;

// import java.io.IOException;

// public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {
    
//     @Override
//     protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//         StringBuilder combinedText = new StringBuilder();
        
//         for (Text value : values) {
//             combinedText.append(value.toString()).append(" ");
//         }
        
//         context.write(key, new Text(combinedText.toString().trim()));
//     }
// }

package Task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PreprocessingReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder combinedText = new StringBuilder();

        for (Text value : values) {
            combinedText.append(value.toString()).append(" ");
        }

        context.write(key, new Text(combinedText.toString().trim()));
    }
}


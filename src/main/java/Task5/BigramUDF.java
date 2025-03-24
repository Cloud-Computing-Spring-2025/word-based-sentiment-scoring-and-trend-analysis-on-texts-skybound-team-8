package Task5;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

public class BigramUDF extends UDF {

    public List<Text> evaluate(Text input) {
        if (input == null || input.toString().isEmpty()) return null;

        String[] words = input.toString().toLowerCase().split("\\s+");
        List<Text> bigrams = new ArrayList<>();

        for (int i = 0; i < words.length - 1; i++) {
            String w1 = words[i].replaceAll("[^a-z]", "");
            String w2 = words[i + 1].replaceAll("[^a-z]", "");
            if (!w1.isEmpty() && !w2.isEmpty()) {
                bigrams.add(new Text(w1 + "_" + w2));
            }
        }

        return bigrams;
    }
}

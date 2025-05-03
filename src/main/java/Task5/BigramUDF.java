package task5;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;
import java.util.HashMap;
import java.util.Map;

public class BigramUDF extends UDF {
    public Map<Text, Integer> evaluate(Text input) {
        if (input == null || input.toString().isEmpty()) return null;

        String[] words = input.toString().toLowerCase().split("\\s+");
        Map<Text, Integer> bigramFreq = new HashMap<>();

        for (int i = 0; i < words.length - 1; i++) {
            String w1 = words[i].replaceAll("[^a-z]", "");
            String w2 = words[i + 1].replaceAll("[^a-z]", "");
            if (!w1.isEmpty() && !w2.isEmpty()) {
                Text bigram = new Text(w1 + "_" + w2);
                bigramFreq.put(bigram, bigramFreq.getOrDefault(bigram, 0) + 1);
            }
        }

        return bigramFreq;
    }
}

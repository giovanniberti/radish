package radish;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeywordRelevantImageReducer extends Reducer<String, Path, String, Path> {
    @Override
    protected void reduce(String key, Iterable<Path> values, Context context) throws IOException, InterruptedException {

    }
}

package radish.batch.kmeans.init;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import radish.batch.kmeans.writables.DoubleVectorsWritable;

import java.io.IOException;

public class SumReducer extends Reducer<NullWritable, DoubleVectorsWritable, NullWritable, DoubleWritable> {
    @Override
    protected void reduce(NullWritable key, Iterable<DoubleVectorsWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;

        for (DoubleVectorsWritable inputValue : values) {
            double distance = ((DoubleWritable) (((ArrayWritable) inputValue.get()[1]).get()[0])).get();
            sum += distance;
        }

        context.write(NullWritable.get(), new DoubleWritable(sum));
    }
}

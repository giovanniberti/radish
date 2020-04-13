package radish.batch.kmeans.init;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.KMeansUtils;
import radish.batch.kmeans.writables.DoubleVectorsWritable;
import radish.utils.HadoopUtils;

import java.io.IOException;
import java.util.Random;

public class CentroidChooser extends Reducer<NullWritable, DoubleVectorsWritable, NullWritable, Text> {
    private double distanceSum;

    public static final Logger logger = LoggerFactory.getLogger(CentroidChooser.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.distanceSum = Double.parseDouble(context.getConfiguration().get(InitPhase.DISTANCE_SUM));
    }

    @Override
    protected void reduce(NullWritable key, Iterable<DoubleVectorsWritable> values, Context context) throws IOException, InterruptedException {
        double factor = new Random().doubles().filter(d -> d > 0).findFirst().getAsDouble();
        double threshold = this.distanceSum * factor;
        double localSum = 0;

        double[] newCentroid = null;
        for (DoubleVectorsWritable inputValue : values) {
            double[] point = HadoopUtils.writableToDoubleArray((ArrayWritable) inputValue.get()[0]);
            double distance = ((DoubleWritable) (((ArrayWritable) inputValue.get()[1]).get()[0])).get();

            newCentroid = point;
            localSum += distance;

            if (localSum >= threshold) {
                break;
            }
        }

        context.write(NullWritable.get(), new Text(KMeansUtils.serializePoint(newCentroid)));
    }
}

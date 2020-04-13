package radish.batch.kmeans;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import radish.batch.kmeans.writables.DoubleArrayWritable;
import radish.utils.HadoopUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * This mapper does the 1st phase of k-means, i.e. the cluster assignment phase.
 * It takes each point and computes for each point the nearest centroid
 * Output format: <centroid, input_point>
 */
public class PointMapper extends Mapper<Object, Text, IntWritable, DoubleArrayWritable> {
    private List<double[]> centroidList;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String[] serializedCentroidList = context.getConfiguration().getStrings(KMeans.CENTROID_LIST_PARAM);
        Base64.Decoder decoder = Base64.getDecoder();
        this.centroidList = Arrays.stream(serializedCentroidList)
                .map(decoder::decode)
                .map(String::new)
                .map(KMeansUtils::deserializePoint)
                .collect(Collectors.toList());
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] pointAndCentroid = value.toString().split(";");
        double[] point = KMeansUtils.deserializePoint(pointAndCentroid[0]);

        EuclideanDistance euclideanDistance = new EuclideanDistance();
        int nearestCentroidIndex = IntStream.range(0, centroidList.size())
                .boxed()
                .min(Comparator.comparingDouble(i -> euclideanDistance.compute(centroidList.get(i), point)))
                .get();

        context.write(new IntWritable(nearestCentroidIndex), HadoopUtils.doubleArrayToWritable(point));
    }
}

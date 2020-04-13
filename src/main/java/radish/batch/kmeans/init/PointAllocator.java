package radish.batch.kmeans.init;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import radish.batch.kmeans.KMeans;
import radish.batch.kmeans.KMeansUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PointAllocator extends Mapper<Object, Text, Text, Text> {
    private List<double[]> centroidList;

    @Override
    protected void setup(Context context) {
        String[] serializedCentroidList = context.getConfiguration().getStrings(KMeans.CENTROID_LIST_PARAM);
        this.centroidList = Arrays.stream(serializedCentroidList)
                .map(KMeansUtils::deserializePoint)
                .collect(Collectors.toList());
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        double[] point = KMeansUtils.deserializePoint(line);

        EuclideanDistance euclideanDistance = new EuclideanDistance();
        int nearestCentroidIndex = IntStream.range(0, centroidList.size())
                .boxed()
                .min(Comparator.comparingDouble(i -> euclideanDistance.compute(centroidList.get(i), point)))
                .get();

        String serializedPoint = KMeansUtils.serializePoint(point);
        context.write(new Text(serializedPoint), new Text(";" + nearestCentroidIndex));
    }
}

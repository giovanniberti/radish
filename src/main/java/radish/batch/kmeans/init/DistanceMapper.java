package radish.batch.kmeans.init;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.writables.DoubleArrayWritable;
import radish.batch.kmeans.KMeans;
import radish.batch.kmeans.KMeansUtils;
import radish.batch.kmeans.writables.DoubleVectorsWritable;
import radish.utils.HadoopUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DistanceMapper extends Mapper<Object, Text, NullWritable, DoubleVectorsWritable> {
    private List<double[]> centroidList;

    public static final Logger logger = LoggerFactory.getLogger(DistanceMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        String[] serializedCentroidList = context.getConfiguration().getStrings(KMeans.CENTROID_LIST_PARAM);
        this.centroidList = Arrays.stream(serializedCentroidList)
                .map(KMeansUtils::deserializePoint)
                .collect(Collectors.toList());
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        double[] point = KMeansUtils.deserializePoint(line);

        if (!centroidList.contains(point)) {
            EuclideanDistance euclideanDistance = new EuclideanDistance();
            double distanceToNearestCentroid = centroidList.stream()
                    .mapToDouble(c -> euclideanDistance.compute(c, point))
                    .min()
                    .getAsDouble();

            DoubleArrayWritable pointWritable = HadoopUtils.doubleArrayToWritable(point);
            DoubleArrayWritable distanceWritable = new DoubleArrayWritable(new DoubleWritable[]{new DoubleWritable(Math.pow(distanceToNearestCentroid, 2))});
            context.write(NullWritable.get(), new DoubleVectorsWritable(new DoubleArrayWritable[]{pointWritable, distanceWritable}));
        }
    }
}

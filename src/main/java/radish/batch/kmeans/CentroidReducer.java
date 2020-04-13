package radish.batch.kmeans;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.writables.DoubleArrayWritable;
import radish.utils.HadoopUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This reducer does the 2nd phase of k-means, i.e. the centroid computation phase
 * It takes a cluster of points in input and computes a new centroid.
 */
public class CentroidReducer extends Reducer<IntWritable, DoubleArrayWritable, Text, Text> {
    private static final Logger logger = LoggerFactory.getLogger(CentroidReducer.class);

    private int dimensions;
    private List<double[]> centroidList;
    private MultipleOutputs<Text, Text> outputContext;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.dimensions = Integer.parseInt(context.getConfiguration().get(KMeans.DIMENSIONS));
        outputContext = new MultipleOutputs<>(context);
        String[] serializedCentroidList = context.getConfiguration().getStrings(KMeans.CENTROID_LIST_PARAM);
        Base64.Decoder decoder = Base64.getDecoder();
        this.centroidList = Arrays.stream(serializedCentroidList)
                .map(decoder::decode)
                .map(String::new)
                .map(KMeansUtils::deserializePoint)
                .collect(Collectors.toList());
        logger.info("centroids list: {} centroids", centroidList.size());
    }

    @Override
    protected void reduce(IntWritable centroidId, Iterable<DoubleArrayWritable> values, Context context) throws IOException, InterruptedException {
        Configuration configuration = context.getConfiguration();
        configuration.set(TextOutputFormat.SEPERATOR, ";");

        EuclideanDistance euclideanDistance = new EuclideanDistance();
        RealVector newCentroid = new ArrayRealVector(new double[this.dimensions]);
        int cardinality = 0;
        double[] nearestMember = null;
        double nearestMemberDistance = Double.POSITIVE_INFINITY;
        for (ArrayWritable memberWritable : values) {
            double[] member = HadoopUtils.writableToDoubleArray(memberWritable);
            newCentroid = newCentroid.add(new ArrayRealVector(member));
            cardinality++;

            double memberDistance = euclideanDistance.compute(member, newCentroid.toArray());
            if (nearestMember == null || memberDistance < nearestMemberDistance) {
                nearestMember = member;
                nearestMemberDistance = memberDistance;
            }

            String serializedMember = KMeansUtils.serializePoint(member);
            outputContext.write(KMeans.POINTS_NAMED_OUTPUT, new Text(serializedMember), new Text(String.valueOf(centroidId.get())));
        }
        newCentroid.mapMultiplyToSelf(1. / cardinality);

        String serializedCentroid = KMeansUtils.serializePoint(newCentroid.toArray());

        if (nearestMember == null) {
            logger.warn("Empty cluster detected.");
            context.write(new Text(serializedCentroid), new Text(";" + KMeans.FIXUP_MARKER));
        } else {
            String serializedNearestMember = KMeansUtils.serializePoint(nearestMember);
            context.write(new Text(serializedCentroid), new Text(";" + serializedNearestMember));

            double[] oldCentroid = centroidList.get(centroidId.get());
            if (euclideanDistance.compute(oldCentroid, newCentroid.toArray()) < KMeans.CONVERGENCE_THRESHOLD) {
                context.getCounter(KMeans.Counters.CONVERGENCE_FLAG_COUNTER).increment(1);
            }
        }
    }
}

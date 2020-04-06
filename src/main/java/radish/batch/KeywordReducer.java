package radish.batch;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static radish.HBaseSchema.*;

public class KeywordReducer extends TableReducer<Text, ImageFeatureData, NullWritable> {
    private static final Logger logger = LoggerFactory.getLogger(KeywordReducer.class);

    private static double[] getNearestCentroid(List<CentroidCluster<DoublePoint>> centroids, DoublePoint point) {
        CentroidCluster<DoublePoint> nearestCentroid = centroids.stream()
                .filter(c -> c.getPoints().contains(point))
                .collect(Collectors.toList())
                .get(0);

        return nearestCentroid.getCenter().getPoint();
    }

    @Override
    protected void reduce(Text key, Iterable<ImageFeatureData> valuesIterable, Context context) throws IOException, InterruptedException {
        logger.info("Start reduce for key {}", key);
        ArrayList<ImageFeatureData> values = new ArrayList<>();

        for (ImageFeatureData featureData : valuesIterable) {
            values.add(new ImageFeatureData(featureData));
        }
        logger.info("Got feature data: {}", values);

        ArrayList<double[]> featureVectors = new ArrayList<>();
        logger.info("Acquiring feature data for {}...", key);
        for (ImageFeatureData featureData : values) {
            double[] featureVector = featureData.features;
            featureVectors.add(featureVector);
        }

        logger.info("Acquired feature data for key {}. {} feature vectors", key, featureVectors.size());
        logger.warn("Sanity check: {}", !featureVectors.stream().allMatch(f -> f == featureVectors.get(0)));
        List<DoublePoint> points = featureVectors.stream()
                .map(DoublePoint::new)
                .collect(Collectors.toList());

        logger.info("Starting k-means for {}", key);
        KMeansPlusPlusClusterer<DoublePoint> clusterer = new KMeansPlusPlusClusterer<>(
                3,
                100_000,
                new EuclideanDistance(),
                new JDKRandomGenerator(),
                KMeansPlusPlusClusterer.EmptyClusterStrategy.FARTHEST_POINT);
        logger.info("Started k-means for {}", key);

        List<CentroidCluster<DoublePoint>> centroids = clusterer.cluster(points);
        List<DoublePoint> clusterCenters = centroids.stream().map(c -> c.getCenter().getPoint()).map(DoublePoint::new).collect(Collectors.toList());
        List<Integer> cardinalities = centroids.stream().map(c -> c.getPoints().size()).collect(Collectors.toList());

        logger.info("Computed k-means clusters for {}", key);
        logger.info("Found cluster centers: {} with cardinalities: {}", clusterCenters, cardinalities);
        logger.info("Images: {}", featureVectors.size());

        for (ImageFeatureData value : values) {
            logger.info("Adding centroid for image {}", value.imageId);
            Put put = new Put(value.imageId.getBytes());
            put.addColumn(DATA_COLUMN_FAMILY, KEYWORD_COLUMN, key.getBytes());

            double[] nearestCentroid = getNearestCentroid(centroids, new DoublePoint(value.features));
            put.addColumn(DATA_COLUMN_FAMILY, NEAREST_CENTROID_COLUMN, HBaseUtils.doubleArrayToBytes(nearestCentroid));

            context.write(NullWritable.get(), put);
        }

        /*
        for (ImageFeatureData value : values) {
            Put put = new Put(value.imageId.getBytes());
            put.addColumn(DATA_COLUMN_FAMILY, KEYWORD_COLUMN, key.getBytes());
            put.addColumn(DATA_COLUMN_FAMILY, NEAREST_CENTROID_COLUMN, "<dummy centroid>".getBytes());

            context.write(NullWritable.get(), put);
        }
        */

        logger.info("#### End reduce for key {}", key);
    }
}

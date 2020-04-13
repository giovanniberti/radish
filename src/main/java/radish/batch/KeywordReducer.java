package radish.batch;

import org.apache.commons.math3.ml.clustering.CentroidCluster;
import org.apache.commons.math3.ml.clustering.DoublePoint;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.KMeans;
import radish.batch.kmeans.KMeansUtils;
import radish.utils.HBaseUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static radish.HBaseSchema.*;

public class KeywordReducer extends TableReducer<Text, ImageFeatureData, NullWritable> {
    private static final Logger logger = LoggerFactory.getLogger(KeywordReducer.class);

    @Override
    protected void reduce(Text key, Iterable<ImageFeatureData> valuesIterable, Context context) {
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
        logger.info("Starting k-means for {}", key);
        try {
            Configuration configuration = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            Path pointsDataPath = new Path("/points_" + key.toString());

            FSDataOutputStream outputStream = fileSystem.create(pointsDataPath, true);
            String contents = featureVectors.stream().map(KMeansUtils::serializePoint).collect(Collectors.joining("\n"));
            outputStream.writeChars(contents);

            logger.info("Started k-means for {}", key);

            List<double[]> centroids = KMeans.cluster(3,
                    100_000,
                    key.toString(),
                    featureVectors.get(0).length,
                    pointsDataPath);

            logger.info("Computed k-means clusters for {}", key);
            logger.info("Found cluster centers: {}", centroids);

            int clusterId = 1;
            EuclideanDistance euclideanDistance = new EuclideanDistance();
            for (double[] clusterCentroid : centroids) {
                logger.info("Persisting cluster center: {}", Arrays.toString(clusterCentroid));

                ImageFeatureData nearestImageFeatureData = values.stream()
                        .min(Comparator.comparing(i -> euclideanDistance.compute(i.features, clusterCentroid)))
                        .get();

                Put put = new Put(new byte[]{(byte) clusterId});
                put.addColumn(DATA_COLUMN_FAMILY, KEYWORD_COLUMN, key.getBytes());
                put.addColumn(DATA_COLUMN_FAMILY, CLUSTER_CENTROID_COLUMN, HBaseUtils.doubleArrayToBytes(clusterCentroid));
                put.addColumn(DATA_COLUMN_FAMILY, NEAREST_POINT_COLUMN, nearestImageFeatureData.imageId.getBytes());

                context.write(NullWritable.get(), put);
                clusterId++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            logger.info("#### End reduce for key {}", key);
        }
    }
}

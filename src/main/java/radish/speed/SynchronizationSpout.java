package radish.speed;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.HBaseSchema;
import radish.utils.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

public class SynchronizationSpout extends BaseRichSpout {
    public static final String SUGAR_CANDY = "sugar_candy";
    public static final String KEYWORD = "keyword";
    public static final String CLUSTER_CENTROIDS = "cluster_centers";
    public static final String CLUSTER_NEAREST_POINTS = "cluster_nearest_points";
    public static final String CLUSTER_NEAREST_POINT_IDS = "cluster_nearest_point_ids";
    public static final String BATCH_TABLE_NAME = "clusters";

    private static final Logger logger = LoggerFactory.getLogger(SynchronizationSpout.class);

    private final String keyword;
    private SpoutOutputCollector collector;
    private long lastBatchCompletedTimestamp = 0;

    public SynchronizationSpout(String keyword) {
        this.keyword = keyword;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(SUGAR_CANDY, KEYWORD, CLUSTER_CENTROIDS, CLUSTER_NEAREST_POINTS, CLUSTER_NEAREST_POINT_IDS));
    }

    @Override
    public void nextTuple() {
        try {
            Scan timestampScan = new Scan(); // HBaseUtils.getScanFilteredByKeyword(this.keyword);
            timestampScan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.NEAREST_POINT_COLUMN);
            timestampScan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN);

            Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
            Table batchTable = connection.getTable(TableName.valueOf(BATCH_TABLE_NAME));

            Result lastTimestampResult = batchTable.getScanner(timestampScan).next();
            Cell latestCell = lastTimestampResult.getColumnLatestCell(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.NEAREST_POINT_COLUMN);

            if (latestCell == null) {
                logger.info("Cluster batch table is empty for keyword " + keyword);
                return;
            }
            long lastTimestamp = latestCell.getTimestamp();

            if (this.lastBatchCompletedTimestamp < lastTimestamp) {
                logger.info("Found more recent batch data");

                Scan clusterCentersScan = new Scan(); // BaseUtils.getScanFilteredByKeyword(keyword);
                clusterCentersScan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.CLUSTER_CENTROID_COLUMN);
                clusterCentersScan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN);
                clusterCentersScan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.NEAREST_POINT_COLUMN);

                ArrayList<Double[]> clusterCentroids = new ArrayList<>();
                ArrayList<Double[]> clusterNearestPoints = new ArrayList<>();
                ArrayList<String> clusterNearestPointIds = new ArrayList<>();
                ResultScanner scanner = batchTable.getScanner(clusterCentersScan);
                for (Result result : scanner) {
                    byte[] clusterCentroidBytes = result.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.CLUSTER_CENTROID_COLUMN);
                    double[] clusterCentroid = HBaseUtils.byteArrayToDoubles(clusterCentroidBytes);
                    clusterCentroids.add(ArrayUtils.toObject(clusterCentroid));

                    byte[] nearestPointId = result.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.NEAREST_POINT_COLUMN);
                    clusterNearestPointIds.add(new String(nearestPointId));

                    Get getNearestPoint = new Get(nearestPointId);
                    Connection joinConnection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                    Table imagesTable = joinConnection.getTable(TableName.valueOf(RadishTopology.IMAGES_TABLE_NAME));
                    Result nearestPointResult = imagesTable.get(getNearestPoint);

                    byte[] nearestPointFeaturesBytes = nearestPointResult.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.FEATURES_COLUMN);
                    Base64.Decoder decoder = Base64.getDecoder();
                    double[] clusterNearestPoint = HBaseUtils.byteArrayToDoubles(decoder.decode(nearestPointFeaturesBytes));
                    clusterNearestPoints.add(ArrayUtils.toObject(clusterNearestPoint));
                }

                collector.emit(new Values(SUGAR_CANDY, keyword, clusterCentroids, clusterNearestPoints, clusterNearestPointIds));
                this.lastBatchCompletedTimestamp = lastTimestamp;
            } else {
                logger.info("New batch not ready: {} >= {}", this.lastBatchCompletedTimestamp, lastTimestamp);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

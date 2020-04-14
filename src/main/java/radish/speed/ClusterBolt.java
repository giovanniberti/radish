package radish.speed;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.utils.HBaseUtils;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Map;

public class ClusterBolt extends BaseStatefulBolt<KeyValueState<String, ClusterBolt.ClusterData>> {
    public static final String CLUSTER_INDEX = "cluster_index";
    public static final String KEYWORD = "keyword";
    public static final String CLUSTER_CENTROID = "cluster_centroid";
    public static final String CLUSTER_NEAREST_MEMBER = "cluster_nearest_member";
    public static final String CLUSTER_NEAREST_MEMBER_ID = "cluster_nearest_member_id";

    private static final Logger logger = LoggerFactory.getLogger(ClusterBolt.class);

    private KeyValueState<String, ClusterData> state;
    private OutputCollector collector;

    static class ClusterData {
        private final ArrayList<Double[]> centroids;
        private final ArrayList<Double[]> nearestMembers;
        private final ArrayList<String> memberIds;

        public ClusterData(ArrayList<Double[]> centroids, ArrayList<Double[]> nearestMembers, ArrayList<String> memberIds) {
            this.centroids = centroids;
            this.nearestMembers = nearestMembers;
            this.memberIds = memberIds;
        }

        public ArrayList<Double[]> getCentroids() {
            return centroids;
        }

        public ArrayList<Double[]> getNearestMembers() {
            return nearestMembers;
        }

        public ArrayList<String> getMemberIds() {
            return memberIds;
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(CLUSTER_INDEX, KEYWORD, CLUSTER_CENTROID, CLUSTER_NEAREST_MEMBER, CLUSTER_NEAREST_MEMBER_ID));
    }

    @Override
    public void initState(KeyValueState<String, ClusterData> state) {
        this.state = state;
    }

    @Override
    public void prepare(Map<String, Object> topologyConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.contains(SynchronizationSpout.SUGAR_CANDY)) {
            logger.info("Received sugar candy");
            ArrayList<Double[]> clusterCentroids = (ArrayList<Double[]>) input.getValueByField(SynchronizationSpout.CLUSTER_CENTROIDS);
            ArrayList<Double[]> clusterNearestPoints = (ArrayList<Double[]>) input.getValueByField(SynchronizationSpout.CLUSTER_NEAREST_POINTS);
            ArrayList<String> clusterNearestPointIds = (ArrayList<String>) input.getValueByField(SynchronizationSpout.CLUSTER_NEAREST_POINT_IDS);
            String keyword = input.getStringByField(SynchronizationSpout.KEYWORD);

            ClusterData newClusterData = new ClusterData(clusterCentroids, clusterNearestPoints, clusterNearestPointIds);
            state.put(keyword, newClusterData);

            emitDataForKeyword(input, keyword);
        } else {
            String keyword = input.getStringByField(FeatureBolt.KEYWORD);
            double[] features = (double[]) input.getValueByField(FeatureBolt.FEATURES);
            String id = input.getStringByField(FeatureBolt.ID);

            double[] belongingClusterNearestPoint = null;
            double[] belongingClusterCentroid = null;
            int clusterIndex = -1;
            double distance = Double.MAX_VALUE;
            EuclideanDistance euclideanDistance = new EuclideanDistance();
            ArrayList<Double[]> centroids = state.get(keyword).getCentroids();
            if (centroids != null) {
                for (int i = 0; i < centroids.size(); i++) {
                    double[] clusterCentroid = ArrayUtils.toPrimitive(centroids.get(i));

                    if (euclideanDistance.compute(features, clusterCentroid) < distance) {
                        belongingClusterCentroid = clusterCentroid;
                        belongingClusterNearestPoint = ArrayUtils.toPrimitive(state.get(keyword).nearestMembers.get(i));
                        clusterIndex = i;
                    }
                }

                if (euclideanDistance.compute(features, belongingClusterCentroid) < euclideanDistance.compute(belongingClusterNearestPoint, belongingClusterCentroid)) {
                    state.get(keyword).nearestMembers.set(clusterIndex, ArrayUtils.toObject(features));
                    state.get(keyword).memberIds.set(clusterIndex, id);
                }

                emitDataForKeyword(input, keyword);
            }
        }
    }

    private void emitDataForKeyword(Tuple input, String keyword) {
        ArrayList<Double[]> clusterCentroids = state.get(keyword).centroids;
        ArrayList<Double[]> clusterNearestPoints = state.get(keyword).nearestMembers;
        ArrayList<String> clusterNearestPointIds = state.get(keyword).memberIds;

        for (int i = 0; i < clusterCentroids.size(); i++) {
            double[] centroid = ArrayUtils.toPrimitive(clusterCentroids.get(i));
            double[] nearestPoint = ArrayUtils.toPrimitive(clusterNearestPoints.get(i));

            Base64.Encoder encoder = Base64.getEncoder();
            String encodedCentroid = new String(encoder.encode(HBaseUtils.doubleArrayToBytes(centroid)));
            String encodedNearestPoint = new String(encoder.encode(HBaseUtils.doubleArrayToBytes(nearestPoint)));
            String nearestPointId = clusterNearestPointIds.get(i);

            collector.emit(input, new Values(keyword + i, keyword, encodedCentroid, encodedNearestPoint, nearestPointId));
            collector.ack(input);
        }
    }
}

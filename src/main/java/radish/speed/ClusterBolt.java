package radish.speed;

import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import radish.utils.HBaseUtils;

import java.util.Base64;
import java.util.Map;

public class ClusterBolt extends BaseStatefulBolt<KeyValueState<String, double[][]>> {
    public static final String CLUSTER_INDEX = "cluster_index";
    public static final String KEYWORD = "keyword";
    public static final String CLUSTER_CENTROID = "cluster_centroid";
    public static final String CLUSTER_NEAREST_MEMBER = "cluster_nearest_member";

    private KeyValueState<String, double[][]> state;
    private OutputCollector collector;

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(CLUSTER_INDEX, KEYWORD, CLUSTER_CENTROID, CLUSTER_NEAREST_MEMBER));
    }

    @Override
    public void initState(KeyValueState<String, double[][]> state) {
        this.state = state;
    }

    @Override
    public void prepare(Map<String, Object> topologyConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        if (input.contains(SynchronizationSpout.SUGAR_CANDY)) {
            double[][] clusterCentroids = (double[][]) input.getValueByField(SynchronizationSpout.CLUSTER_CENTROIDS);
            double[][] clusterNearestPoints = (double[][]) input.getValueByField(SynchronizationSpout.CLUSTER_NEAREST_POINTS);
            String keyword = input.getStringByField(SynchronizationSpout.KEYWORD);

            state.put(keyword + "_centroids", clusterCentroids);
            state.put(keyword + "_nearest_points", clusterNearestPoints);

            emitDataForKeyword(input, keyword);
        } else {
            String keyword = input.getStringByField(FeatureBolt.KEYWORD);
            double[] features = (double[]) input.getValueByField(FeatureBolt.FEATURES);

            double[] belongingClusterNearestPoint = null;
            double[] belongingClusterCentroid = null;
            int clusterIndex = -1;
            double distance = Double.MAX_VALUE;
            EuclideanDistance euclideanDistance = new EuclideanDistance();
            double[][] centroids = state.get(keyword + "_centroids");
            if (centroids != null) {
                for (int i = 0; i < centroids.length; i++) {
                    double[] clusterCentroid = centroids[i];

                    if (euclideanDistance.compute(features, clusterCentroid) < distance) {
                        belongingClusterCentroid = clusterCentroid;
                        belongingClusterNearestPoint = state.get(keyword + "_nearest_points")[i];
                        clusterIndex = i;
                    }
                }

                if (euclideanDistance.compute(features, belongingClusterCentroid) < euclideanDistance.compute(belongingClusterNearestPoint, belongingClusterCentroid)) {
                    double[][] nearestPoints = state.get(keyword + "_nearest_points");
                    nearestPoints[clusterIndex] = features;
                    state.put(keyword + "_nearest_points", nearestPoints);
                }

                emitDataForKeyword(input, keyword);
            }
        }
    }

    private void emitDataForKeyword(Tuple input, String keyword) {
        double[][] clusterCentroids = state.get(keyword + "_centroids");
        double[][] clusterNearestPoints = state.get(keyword + "_nearest_points");

        for (int i = 0; i < clusterCentroids.length; i++) {
            double[] centroid = clusterCentroids[i];
            double[] nearestPoint = clusterNearestPoints[i];

            Base64.Encoder encoder = Base64.getEncoder();
            String encodedCentroid = new String(encoder.encode(HBaseUtils.doubleArrayToBytes(centroid)));
            String encodedNearestPoint = new String(encoder.encode(HBaseUtils.doubleArrayToBytes(nearestPoint)));

            collector.emit(input, new Values(i, keyword, encodedCentroid, encodedNearestPoint));
            collector.ack(input);
        }
    }
}

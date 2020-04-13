package radish.speed;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.utils.HBaseUtils;

import java.util.Base64;
import java.util.Map;

public class FeatureMapperBolt extends BaseRichBolt {
    public static final String ID = FeatureBolt.ID;
    public static final String FEATURES = FeatureBolt.FEATURES;
    public static final String KEYWORD = FeatureBolt.KEYWORD;

    private static final Logger logger = LoggerFactory.getLogger(FeatureMapperBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID, KEYWORD, FEATURES));
    }

    @Override
    public void execute(Tuple input) {
        String id = input.getStringByField(FeatureBolt.ID);
        String keyword = input.getStringByField(FeatureBolt.KEYWORD);
        double[] featureVector = (double[]) input.getValueByField(FeatureBolt.FEATURES);

        logger.info("Persisting features: "  + featureVector);

        Base64.Encoder encoder = Base64.getEncoder();
        String encodedFeatureVector = new String(encoder.encode(HBaseUtils.doubleArrayToBytes(featureVector)));

        collector.emit(input, new Values(id, keyword, encodedFeatureVector));
        collector.ack(input);
    }
}

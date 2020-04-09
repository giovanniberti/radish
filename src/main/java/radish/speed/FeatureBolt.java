package radish.speed;

import net.semanticmetadata.lire.imageanalysis.features.GlobalFeature;
import net.semanticmetadata.lire.imageanalysis.features.global.CEDD;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.Config;
import radish.HBaseSchema;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class FeatureBolt extends BaseRichBolt {
    public static final String ID = new String(HBaseSchema.ID_COLUMN);
    public static final String KEYWORD = new String(HBaseSchema.KEYWORD_COLUMN);
    public static final String FEATURES = new String(HBaseSchema.FEATURES_COLUMN);

    private static final Logger logger = LoggerFactory.getLogger(FeatureBolt.class);
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
        String imageId = input.getStringByField(DownloadBolt.ID);
        String keyword = input.getStringByField(DownloadBolt.KEYWORD);
        String rawImagePath = input.getStringByField(DownloadBolt.IMAGE_PATH);

        try {
            FileSystem fs = FileSystem.get(Config.getInstance().hadoopConfiguration);
            Path imagePath = new Path(rawImagePath);

            InputStream imageInputStream = fs.open(imagePath);
            logger.debug("imageInputStream = {}", imageInputStream);

            BufferedImage imageData = ImageIO.read(imageInputStream);
            logger.debug("imageData = {}", imageData);

            GlobalFeature featureExtractor = new CEDD();
            featureExtractor.extract(imageData);
            double[] featureVector = featureExtractor.getFeatureVector();
            logger.info("Image {}: extracted feature vector {}", imageId, featureVector);

            collector.emit(new Values(imageId, keyword, featureVector));
            collector.ack(input);
        } catch (IOException e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }
}

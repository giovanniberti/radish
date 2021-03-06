package radish.speed;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Map;
import java.util.UUID;

public class DownloadBolt extends BaseRichBolt {
    public static final String ID = new String(HBaseSchema.ID_COLUMN);
    public static final String KEYWORD = new String(HBaseSchema.KEYWORD_COLUMN);
    public static final String IMAGE_PATH = new String(HBaseSchema.IMAGE_PATH_COLUMN);

    private static final Logger logger = LoggerFactory.getLogger(DownloadBolt.class);
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(ID, KEYWORD, IMAGE_PATH));
    }

    @Override
    public void execute(Tuple input) {
        logger.info("Processing tuple " + input);
        String keyword = input.getStringByField(TwitterSpout.KEYWORD);
        String rawURL = input.getStringByField(TwitterSpout.IMAGE_URL);

        Configuration config = Config.getInstance().hadoopConfiguration;

        try {
            FileSystem fileSystem = FileSystem.get(config);
            URL url = new URL(rawURL);
            Path path = new Path(String.valueOf(Paths.get("/images/", keyword, url.getFile())));

            try (InputStream imageStream = url.openStream(); FSDataOutputStream fileStream = fileSystem.create(path)) {
                IOUtils.copy(imageStream, fileStream);
                fileStream.hsync();
            }

            logger.info("Finished downloading: " + path);
            collector.emit(input, new Values(UUID.randomUUID().toString(), keyword, path.toString()));
            collector.ack(input);
        } catch (IOException e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }
}
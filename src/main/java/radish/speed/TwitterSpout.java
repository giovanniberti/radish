package radish.speed;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.Config;
import twitter4j.ExtendedMediaEntity;
import twitter4j.MediaEntity;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterSpout extends BaseRichSpout {
    private static final Logger logger = LoggerFactory.getLogger(TwitterSpout.class);
    private final String keyword;
    private final AccessToken accessToken;

    private SpoutOutputCollector collector;
    private LinkedBlockingQueue<String> urlsToProcess;
    private TwitterStream stream;

    public TwitterSpout(String keyword, AccessToken accessToken) {
        this.keyword = keyword;
        this.accessToken = accessToken;
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        urlsToProcess = new LinkedBlockingQueue<>();

        Config radishConf = Config.getInstance();

        stream = TwitterStreamFactory.getSingleton();
        stream.setOAuthConsumer(radishConf.apiKey, radishConf.apiSecretKey);
        stream.setOAuthAccessToken(accessToken);

        stream.onStatus(status -> {
            logger.info("### RECEIVED STATUS");

            for (ExtendedMediaEntity extendedMediaEntity : status.getExtendedMediaEntities()) {
                logger.debug("Found extended entity with type " + extendedMediaEntity.getType());
                if (extendedMediaEntity.getType().equals("photo")) {
                    urlsToProcess.add(extendedMediaEntity.getMediaURL());
                }
            }

            for (MediaEntity mediaEntity : status.getMediaEntities()) {
                logger.debug("Found entity with type " + mediaEntity.getType());
                if (mediaEntity.getType().equals("photo")) {
                    urlsToProcess.add(mediaEntity.getMediaURL());
                }
            }

            logger.debug("Current urls: " + urlsToProcess);
        });
        stream.filter(keyword);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "image_url"));
    }

    @Override
    public void nextTuple() {
        for (Iterator<String> iterator = urlsToProcess.iterator(); iterator.hasNext(); ) {
            String url = iterator.next();
            logger.info("~~~ EMITTING tuple: " + keyword + " " + url);
            collector.emit(new Values(keyword, url));

            iterator.remove();
        }
    }

    @Override
    public void close() {
        stream.shutdown();
    }
}

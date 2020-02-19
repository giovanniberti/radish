package radish.speed;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import winterwell.jtwitter.AStream;
import winterwell.jtwitter.Twitter;
import winterwell.jtwitter.TwitterEvent;
import winterwell.jtwitter.TwitterStream;

import java.util.*;
import java.util.stream.Collectors;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String keyword;
    private List<String> urlsToProcess = new ArrayList<>();

    public TwitterSpout(String keyword, TwitterStream stream) {
        this.keyword = keyword;

        List<String> keywordList = new ArrayList<>();
        keywordList.add(keyword);
        stream.setTrackKeywords(keywordList);

        stream.addListener(new AStream.IListen() {
            @Override
            public boolean processEvent(TwitterEvent twitterEvent) throws Exception {
                return true;
            }

            @Override
            public boolean processSystemEvent(Object[] objects) throws Exception {
                return true;
            }

            @Override
            public boolean processTweet(Twitter.ITweet tweet) throws Exception {

                List<Twitter.TweetEntity> tweetMedias = tweet.getTweetEntities(Twitter.KEntityType.media);

                List<String> mediaUrls = tweetMedias
                        .stream()
                        .map(Twitter.TweetEntity::mediaUrl)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());

                urlsToProcess.addAll(mediaUrls);

                return true;
            }
        });
    }

    @Override
    public void open(Map<String, Object> conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "image_url"));
    }

    @Override
    public void nextTuple() {
        for (Iterator<String> iterator = urlsToProcess.iterator(); iterator.hasNext(); ) {
            String url = iterator.next();
            collector.emit(new Values(keyword, url));

            iterator.remove();
        }
    }
}

package radish.speed;

import clojure.lang.IFn;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import radish.Config;
import winterwell.jtwitter.OAuthSignpostClient;
import winterwell.jtwitter.Twitter;
import winterwell.jtwitter.TwitterStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RadishTopology {
    private static final String ACCESS_TOKEN = Config.getInstance().accessToken;
    private static final String ACCESS_TOKEN_SECRET = Config.getInstance().accessTokenSecret;
    private static final String API_KEY = Config.getInstance().apiKey;
    private static final String API_SECRET_KEY = Config.getInstance().apiSecretKey;

    private static final String TWITTER_SPOUT = "TWITTER_SPOUT";
    private static final String DOWNLOAD_BOLT = "DOWNLOAD_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";


    public static void main(String[] args) throws Exception {
        args = new String[]{"apple"};
        String keyword = args[0];

        org.apache.storm.Config conf = new org.apache.storm.Config();
        TopologyBuilder builder = new TopologyBuilder();

        OAuthSignpostClient client = new OAuthSignpostClient(API_KEY, API_SECRET_KEY, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
        Twitter twitter = new Twitter("bertigiova", client);
        twitter.setIncludeTweetEntities(true);

        TwitterStream stream = new TwitterStream(twitter);

        builder.setSpout(TWITTER_SPOUT, new TwitterSpout(keyword, stream));
        builder.setBolt(DOWNLOAD_BOLT, new DownloadBolt()).shuffleGrouping(TWITTER_SPOUT);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("keyword")
                .withColumnFields(new Fields("keyword", "image_path"))
                .withColumnFamily("data");


        Map<String, String> hBaseConfig = new HashMap<>();
        hBaseConfig.put("hbase.rootdir", "hdfs://namenode:9000/hbase");
        hBaseConfig.put("hbase.zookeeper.quorum", "zoo");

        conf.put("hbase.config", hBaseConfig);

        HBaseBolt hBaseBolt = new HBaseBolt("radish", mapper)
                .withConfigKey("hbase.config");

        builder.setBolt(HBASE_BOLT, hBaseBolt).shuffleGrouping(DOWNLOAD_BOLT);

        List<String> nimbusSeeds = new ArrayList<>();
        nimbusSeeds.add("nimbus");
        conf.put("nimbus.seeds", nimbusSeeds);

        List<String> zookeeperServers = new ArrayList<>();
        zookeeperServers.add("zoo");
        conf.put("storm.zookeeper.servers", zookeeperServers);

        StormTopology topology = builder.createTopology();

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("RADISH_TOPOLOGY", conf, topology);
    }
}

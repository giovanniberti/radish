package radish.speed;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.KillOptions;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import radish.Config;
import twitter4j.auth.AccessToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RadishTopology extends ConfigurableTopology {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RadishTopology.class);

    private static final String TWITTER_SPOUT = "TWITTER_SPOUT";
    private static final String DOWNLOAD_BOLT = "DOWNLOAD_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";

    public static void main(String[] args) throws Exception {
        String keyword = "apple";

        Options options = new Options();

        OptionGroup stormMode = new OptionGroup();

        Option local = new Option("l", "local", false, "set local mode (use LocalCluster)");
        local.setRequired(false);
        stormMode.addOption(local);

        Option distributed = new Option("d", "distributed", false, "set locally distributed mode (to use with Docker)");
        distributed.setRequired(false);
        stormMode.addOption(distributed);

        options.addOptionGroup(stormMode);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("radish", options);

            System.exit(1);
        }

        boolean isDistributed = cmd.hasOption(distributed.getOpt());

        org.apache.storm.Config conf = new org.apache.storm.Config();
        initConfig(conf, true);

        TopologyBuilder builder = initRadishTopologyBuilder(keyword);

        if (isDistributed) {
            ConfigurableTopology topology = new RadishTopology();
            args = new String[]{keyword};

            ConfigurableTopology.start(topology, args);
        } else {
            StormTopology topology = builder.createTopology();
            try (LocalCluster localCluster = new LocalCluster()) {
                String topologyName = "RADISH_TOPOLOGY";

                logger.info("Starting " + topologyName + "in local mode");

                localCluster.submitTopology(topologyName, conf, topology);
                Thread.sleep(250 * 1000);
                KillOptions killOptions = new KillOptions();
                killOptions.set_wait_secs(1);
                localCluster.killTopologyWithOpts(topologyName, killOptions);
            }
        }
    }

    public int run(String[] keywords) {
        org.apache.storm.Config conf = new org.apache.storm.Config();
        initConfig(conf, false);

        TopologyBuilder builder = initRadishTopologyBuilder(keywords[0]);

        return submit("RADISH_TOPOLOGY", conf, builder);
    }

    private static void initConfig(org.apache.storm.Config conf, boolean isLocalCluster) {
        Map<String, String> hBaseConfig = new HashMap<>();

        // conf.setDebug(true);

        if (isLocalCluster) {
            hBaseConfig.put("hbase.rootdir", "hdfs://localhost:9000/hbase");
            hBaseConfig.put("hbase.zookeeper.quorum", "localhost");
        } else {
            hBaseConfig.put("hbase.rootdir", "hdfs://namenode:9000/hbase");
            hBaseConfig.put("hbase.zookeeper.quorum", "zoo");

            List<String> nimbusSeeds = new ArrayList<>();
            nimbusSeeds.add("nimbus");
            conf.put("nimbus.seeds", nimbusSeeds);

            List<String> zookeeperServers = new ArrayList<>();
            zookeeperServers.add("zoo");
            conf.put("storm.zookeeper.servers", zookeeperServers);
        }

        conf.put("hbase.config", hBaseConfig);
    }

    @NotNull
    private static TopologyBuilder initRadishTopologyBuilder(String keyword) {
        TopologyBuilder builder = new TopologyBuilder();

        Config radishConf = Config.getInstance();

        AccessToken accessToken = new AccessToken(radishConf.accessToken, radishConf.accessTokenSecret);

        builder.setSpout(TWITTER_SPOUT, new TwitterSpout(keyword, accessToken));

        builder.setBolt(DOWNLOAD_BOLT, new DownloadBolt()).shuffleGrouping(TWITTER_SPOUT);

        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("id")
                .withColumnFields(new Fields("id", "keyword", "image_path"))
                .withColumnFamily("data");

        HBaseBolt hBaseBolt = new HBaseBolt("radish", mapper)
                .withConfigKey("hbase.config");

        builder.setBolt(HBASE_BOLT, hBaseBolt).shuffleGrouping(DOWNLOAD_BOLT);
        return builder;
    }
}

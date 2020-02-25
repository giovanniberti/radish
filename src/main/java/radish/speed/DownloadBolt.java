package radish.speed;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;
import java.util.UUID;

public class DownloadBolt extends BaseRichBolt {
    private OutputCollector collector;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("keyword", "image_path"));
    }

    @Override
    public void execute(Tuple input) {
        String keyword = input.getStringByField("keyword");
        String rawURL = input.getStringByField("url");

        Configuration config = new Configuration();
        try {
            FileSystem fileSystem = FileSystem.get(config);
            URL url = new URL(rawURL);
            Path path = new Path("/images/" + keyword + "/" + url.getFile());

            try (InputStream imageStream = url.openStream(); OutputStream fileStream = fileSystem.create(path).getWrappedStream()) {
                IOUtils.copy(imageStream, fileStream);
            }

            collector.emit(new Values(keyword, path.toString()));
            collector.ack(input);
        } catch (IOException e) {
            collector.fail(input);
            e.printStackTrace();
        }
    }
}
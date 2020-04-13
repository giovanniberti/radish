package radish.batch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import radish.HBaseSchema;

public class Radish extends Configured implements Tool {
    private static final String IMAGES_TABLE = "images";
    public static final String CLUSTERS_TABLE = "clusters";

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        Job job = Job.getInstance(conf, "twitter_imgrel");
        job.setJarByClass(Radish.class);

        Scan scan = new Scan();
        scan.addFamily(HBaseSchema.DATA_COLUMN_FAMILY);
        TableMapReduceUtil.initTableMapperJob(
                IMAGES_TABLE,
                scan,
                ImageDBMapper.class,
                Text.class,
                ImageFeatureData.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(CLUSTERS_TABLE, KeywordReducer.class, job);

        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int result = ToolRunner.run(new Radish(), args);
        System.exit(result);
    }
}

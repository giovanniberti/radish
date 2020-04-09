package radish.batch;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.utils.HBaseUtils;

import java.io.IOException;
import java.util.Base64;

import static radish.HBaseSchema.*;

public class ImageDBMapper extends TableMapper<Text, ImageFeatureData> {
    private static final Logger logger = LoggerFactory.getLogger(ImageDBMapper.class);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        byte[] keywordData = value.getValue(DATA_COLUMN_FAMILY, KEYWORD_COLUMN);
        Text keyword = new Text(new String(keywordData));

        byte[] idData = value.getValue(DATA_COLUMN_FAMILY, ID_COLUMN);
        String imageId = new String(idData);

        logger.info("Mapping over image id: {}", imageId);

        byte[] featureData = value.getValue(DATA_COLUMN_FAMILY, FEATURES_COLUMN);
        Base64.Decoder decoder = Base64.getDecoder();

        double[] features = HBaseUtils.byteArrayToDoubles(decoder.decode(featureData));

        ImageFeatureData imageFeatureData = new ImageFeatureData(imageId, features);
        context.write(keyword, imageFeatureData);
    }
}

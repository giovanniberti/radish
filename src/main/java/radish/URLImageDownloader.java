package radish;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.List;

public class URLImageDownloader extends Mapper<Object, Text, Text, Text> {
    @Override
    protected void map(Object key, Text keyword, Context context) throws IOException, InterruptedException {
        TwitterImageCrawler crawler = new TwitterImageCrawler();
        List<String> mediaUrls = crawler.crawlKeyword(keyword.toString());

        for (String rawURL : mediaUrls) {
            URL url = new URL(rawURL);

            Text rawPath = new Text("/images/" + keyword + url.getFile());
            Path path = new Path(rawPath.toString());
            FileSystem fileSystem = path.getFileSystem(context.getConfiguration());

            try (InputStream imageStream = url.openStream(); OutputStream fileStream = fileSystem.create(path).getWrappedStream()) {
                IOUtils.copy(imageStream, fileStream);
            }

            context.write(new Text(rawURL), rawPath);
        }
    }
}

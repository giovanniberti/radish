package radish.apiserver;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.ModelAndView;

import java.io.IOException;
import java.util.*;

@Controller
public class IndexController {
    private static final Logger logger = LoggerFactory.getLogger(IndexController.class);

    @GetMapping("/")
    public ModelAndView main() throws IOException {
        ModelAndView mav = new ModelAndView("index");
        List<String> keywords = getKeywords();

        Map<String, Integer> numClusters = new HashMap<>();
        for (String keyword : keywords) {
            numClusters.put(keyword, getClusterCardinality(keyword));
        }

        mav.addObject("cards", numClusters);
        mav.addObject("keywords", keywords);
        return mav;
    }

    public int getClusterCardinality(String keyword) throws IOException {
        Configuration hBaseConfig = getConnectionConfig();

        Connection conn = ConnectionFactory.createConnection(hBaseConfig);
        TableName tableName = TableName.valueOf("clusters");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN);
        ResultScanner results = table.getScanner(scan);

        int num = 0;
        for (Result result : results) {
            if (Arrays.equals(result.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN), keyword.getBytes())) {
                num++;
            }
        }

        return num;
    }

    @GetMapping("/keywords")
    public List<String> getKeywords() throws IOException {
        Configuration hBaseConfig = getConnectionConfig();

        Connection conn = ConnectionFactory.createConnection(hBaseConfig);
        TableName tableName = TableName.valueOf("clusters");
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();
        scan.addColumn(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN);
        ResultScanner results = table.getScanner(scan);

        Set<String> keywords = new HashSet<>();
        for (Result result : results) {
            String keywordColumn = new String(result.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.KEYWORD_COLUMN));
            keywords.add(keywordColumn);
        }

        conn.close();
        return List.copyOf(keywords);
    }



    private Configuration getConnectionConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.rootdir", "hdfs://localhost:9000/hbase");
        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("fs.defaultFS", "hdfs://localhost:9000");

        return config;
    }

    @GetMapping("/cluster_image")
    public ResponseEntity<byte[]> getClusterImage(@RequestParam("keyword") String keyword,
                                                  @RequestParam("index") int index) throws IOException {
        final HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.IMAGE_PNG);

        Configuration conf = getConnectionConfig();

        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tableName = TableName.valueOf("clusters_speed");
        Table speedTable = conn.getTable(tableName);

        Result result = speedTable.get(new Get((keyword + index).getBytes()));
        if (result == null) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "image not found");
        }

        byte[] imageId = result.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.CLUSTER_NEAREST_MEMBER_ID);

        TableName imagesTableName = TableName.valueOf("images");
        Table imagesTable = conn.getTable(imagesTableName);

        Result imageResult = imagesTable.get(new Get(imageId));
        String imagePath = new String(imageResult.getValue(HBaseSchema.DATA_COLUMN_FAMILY, HBaseSchema.IMAGE_PATH_COLUMN));

        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream inputStream = fileSystem.open(new Path(imagePath));

        return new ResponseEntity<>(IOUtils.toByteArray(inputStream), headers, HttpStatus.OK);
    }

    @PostMapping("/add_keyword")
    public void addKeyword(@RequestParam(value = "keyword") String keyword) { }
}

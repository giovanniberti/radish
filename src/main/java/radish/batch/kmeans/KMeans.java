package radish.batch.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.init.InitPhase;
import radish.batch.kmeans.writables.DoubleArrayWritable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class KMeans extends Configured implements Tool {
    public static final String INPUT_KEYWORD = "radish.kmeans.keyword";
    public static final String POINTS_DATA_PATH = "radish.kmeans.points_data";
    public static final String POINTS_INPUT_PATH = "radish.kmeans.points_input_path";
    public static final String DIMENSIONS = "radish.kmeans.dimensions";
    public static final String K = "radish.kmeans.k";

    public static final String CENTROID_LIST_PARAM = "centroid_list";
    public static final String FIXUP_MARKER = "fixup";

    public static final Path DEFAULT_OUTPUT_DIR = new Path("/clusters/");
    public static final String POINTS_NAMED_OUTPUT = "pointsNamedOutput";
    public static final double CONVERGENCE_THRESHOLD = 0.001;

    private static final Logger logger = LoggerFactory.getLogger(KMeans.class);

    enum Counters {
        CONVERGENCE_FLAG_COUNTER
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();

        String keyword = configuration.get(INPUT_KEYWORD);

        FileSystem fileSystem = FileSystem.get(configuration);
        Path centroidsFilePath = Path.mergePaths(DEFAULT_OUTPUT_DIR, new Path(InitPhase.INIT_CHOICE_FILENAME_PREFIX + keyword));

        if (!fileSystem.exists(centroidsFilePath)) {
            logger.info("No centroids file found. Starting initialization phase");
            fileSystem.create(centroidsFilePath).close();

            int result = ToolRunner.run(configuration, new InitPhase(), new String[]{});
            if (result != 0) {
                throw new RuntimeException("Error while starting InitPhase. Return code: " + result);
            }
        }

        FSDataInputStream inputStream = fileSystem.open(centroidsFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        Base64.Encoder encoder = Base64.getEncoder();
        String[] serializedCentroids = reader.lines()
                .map(l -> l.split(";")[0])
                .map(String::getBytes)
                .map(encoder::encodeToString)
                .toArray(String[]::new);
        inputStream.close();
        fileSystem.delete(centroidsFilePath, true);
        configuration.setStrings(CENTROID_LIST_PARAM, serializedCentroids);

        if (serializedCentroids.length == 0) {
            throw new RuntimeException("Centroids file (" + centroidsFilePath.toUri() + ") empty");
        }

        logger.info("Starting k-means iteration... ({} centroids)", serializedCentroids.length);

        Job job = Job.getInstance(configuration, "kmeans_" + keyword);
        job.setJarByClass(KMeans.class);
        job.setMapperClass(PointMapper.class);
        job.setReducerClass(CentroidReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(DoubleArrayWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path pointsInputPath = new Path(configuration.get(KMeans.POINTS_INPUT_PATH));
        FileInputFormat.addInputPath(job, pointsInputPath);
        FileOutputFormat.setOutputPath(job, centroidsFilePath);

        MultipleOutputs.addNamedOutput(job, POINTS_NAMED_OUTPUT, TextOutputFormat.class, Text.class, Text.class);

        boolean success = job.waitForCompletion(true);

        if (!success) {
            throw new RuntimeException("Iteration failed");
        }

        Path newPointsFile = Path.mergePaths(centroidsFilePath, new Path("/part-r-00000"));
        Path tmpPointsFile = Path.mergePaths(centroidsFilePath.getParent(), new Path("/centroids.bak"));
        logger.info("Moving {} to {}", newPointsFile.toUri(), tmpPointsFile.toUri());
        fileSystem.rename(newPointsFile, tmpPointsFile);

        Path newCentroidsFile = Path.mergePaths(centroidsFilePath, new Path("/pointsNamedOutput-r-00000"));
        Path tmpCentroidsFile = Path.mergePaths(centroidsFilePath.getParent(), new Path("/points.bak"));
        fileSystem.rename(newCentroidsFile, tmpCentroidsFile);

        fileSystem.delete(centroidsFilePath, true);
        fileSystem.delete(pointsInputPath, true);
        fileSystem.rename(tmpCentroidsFile, pointsInputPath);
        fileSystem.rename(tmpPointsFile, centroidsFilePath);

        long convergedClustersCounter = job.getCounters().findCounter(Counters.CONVERGENCE_FLAG_COUNTER).getValue();

        int convergedClusters = Math.toIntExact(convergedClustersCounter);
        int k = Integer.parseInt(configuration.get(K));

        logger.info("k-means iteration completed. Converged centroids: " + convergedClustersCounter + "/" + k);

        fixUpCentroids(pointsInputPath);

        return convergedClusters;
    }

    private void fixUpCentroids(Path centroidsFilePath) throws Exception {
        logger.info("Fixing centroids on: {}", centroidsFilePath.toUri());
        Configuration configuration = getConf();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataInputStream inputStream = fileSystem.open(centroidsFilePath);
        String centroidsFileContents = inputStream.readUTF();

        List<String> correctCentroids = Arrays.stream(centroidsFileContents.split("\n"))
                .map(line -> line.split(";"))
                .filter(components -> components.length > 1 && !components[1].equals(FIXUP_MARKER))
                .map(components -> components[0])
                .collect(Collectors.toList());

        int k = Integer.parseInt(configuration.get(K));

        if (correctCentroids.size() < k) {
            logger.info("Fixing centroids: " + (k - correctCentroids.size()) + "/" + k + " to fix");
            configuration.setStrings(InitPhase.INITAL_CENTROIDS, ((String[]) correctCentroids.toArray()));
            int result = ToolRunner.run(configuration, new InitPhase(), new String[]{});

            if (result != 0) {
                throw new RuntimeException("Error while starting InitPhase. Return code: " + result);
            }
        }
    }

    public static List<double[]> cluster(int k, int maxIterations, String keyword, int dimensions, Path pointsDataPath) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(K, String.valueOf(k));
        configuration.set(INPUT_KEYWORD, keyword);
        configuration.set(DIMENSIONS, String.valueOf(dimensions));
        configuration.set(POINTS_DATA_PATH, pointsDataPath.toUri().getRawPath());
        configuration.set(POINTS_INPUT_PATH, Path.mergePaths(DEFAULT_OUTPUT_DIR, new Path("/input_" + keyword)).toUri().getRawPath());

        logger.info("called cluster() with conf: INPUT_KEYWORD {}; POINTS_DATA_PATH {}; POINTS_INPUT_PATH {}",
                keyword, configuration.get(POINTS_DATA_PATH), configuration.get(POINTS_INPUT_PATH));

        int converged = 0;
        int numIterations = 0;
        while (converged < k && numIterations < maxIterations) {
            converged = ToolRunner.run(configuration, new KMeans(), new String[]{});
            numIterations++;
            logger.info("{} centroids converged. # iterations: {}", converged, numIterations);
        }

        FileSystem fileSystem = FileSystem.get(configuration);
        Path centroidsFilePath = Path.mergePaths(DEFAULT_OUTPUT_DIR, new Path(InitPhase.INIT_CHOICE_FILENAME_PREFIX + keyword));
        FSDataInputStream inputStream = fileSystem.open(centroidsFilePath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        return reader.lines()
                .map(l -> l.split(";")[0])
                .map(KMeansUtils::deserializePoint)
                .collect(Collectors.toList());
    }

    public static void main(String[] args) throws Exception {
        String key = args[0];
        Path pointsDataPath = new Path(args[1]);

        System.out.println("Starting kmeans on " + pointsDataPath.toUri());

        List<double[]> centroids = KMeans.cluster(3,
                100_000,
                key,
                2,
                pointsDataPath);

        System.out.println("Found centroids: " + centroids.stream().map(Arrays::toString).collect(Collectors.joining()));
    }
}

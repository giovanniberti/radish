package radish.batch.kmeans.init;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import radish.batch.kmeans.writables.DoubleArrayWritable;
import radish.batch.kmeans.KMeans;
import radish.batch.kmeans.KMeansUtils;
import radish.batch.kmeans.writables.DoubleVectorsWritable;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class InitPhase extends Configured implements Tool {
    public static final String INIT_SUM_FILENAME_PREFIX = "/init_sum_";
    public static final String INIT_CHOICE_FILENAME_PREFIX = "/kmeans_centroids_";
    public static final String DISTANCE_SUM = "radish.kmeans.init_phase_distance_sum";
    public static final String INITAL_CENTROIDS = "radish.kmeans.initial_centroids";

    public static final Path DEFAULT_OUTPUT_DIR = new Path("/clusters/init/");

    private static final Logger logger = LoggerFactory.getLogger(InitPhase.class);

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();

        int k = Integer.parseInt(configuration.get(KMeans.K));

        Path pointsDataPath = new Path(configuration.get(KMeans.POINTS_DATA_PATH));
        FileSystem fileSystem = FileSystem.get(configuration);

        FSDataInputStream inputStream = fileSystem.open(pointsDataPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String firstLine = reader.readLine();
        List<double[]> centroids = new ArrayList<>();

        String[] initialCentroids = configuration.getStrings(INITAL_CENTROIDS);
        if (initialCentroids != null) {
            centroids = Arrays.stream(initialCentroids).map(KMeansUtils::deserializePoint).collect(Collectors.toList());
        } else {
            double[] initialPoint = KMeansUtils.deserializePoint(firstLine);
            centroids.add(initialPoint);
        }

        String keyword = configuration.get(KMeans.INPUT_KEYWORD);
        Path choiceFilePath = Path.mergePaths(KMeans.DEFAULT_OUTPUT_DIR, new Path(INIT_CHOICE_FILENAME_PREFIX + keyword));

        boolean ok;
        while (centroids.size() < k) {
            String[] serializedCentroids = centroids.stream()
                    .map(arr -> Arrays.stream(arr).mapToObj(String::valueOf).collect(Collectors.joining(" ")))
                    .toArray(String[]::new);

            configuration.setStrings(KMeans.CENTROID_LIST_PARAM, serializedCentroids);

            logger.info("Init phase: starting distance computing ({}/{})", centroids.size() + 1, k);

            Job sumJob = Job.getInstance(configuration, "kmeans_init_sum_" + keyword);
            sumJob.setJarByClass(InitPhase.class);
            sumJob.setMapperClass(DistanceMapper.class);
            sumJob.setReducerClass(SumReducer.class);
            sumJob.setMapOutputKeyClass(NullWritable.class);
            sumJob.setMapOutputValueClass(DoubleVectorsWritable.class);
            sumJob.setOutputKeyClass(NullWritable.class);
            sumJob.setOutputValueClass(DoubleWritable.class);

            Path sumFilePath = Path.mergePaths(DEFAULT_OUTPUT_DIR, new Path(INIT_SUM_FILENAME_PREFIX + keyword));

            FileInputFormat.addInputPath(sumJob, pointsDataPath);
            FileOutputFormat.setOutputPath(sumJob, sumFilePath);

            fileSystem.delete(sumFilePath, true);
            ok = sumJob.waitForCompletion(true);
            if (!ok) {
                return -1;
            }

            FSDataInputStream sumInputStream = fileSystem.open(Path.mergePaths(sumFilePath, new Path("/part-r-00000")));
            BufferedReader sumReader = new BufferedReader(new InputStreamReader(sumInputStream));
            String distancesSumString = sumReader.readLine();
            configuration.set(DISTANCE_SUM, distancesSumString);

            logger.info("Init phase: ended distance computing ({}/{})", centroids.size() + 1, k);
            logger.info("Init phase: starting centroid assignment ({}/{})", centroids.size() + 1, k);

            Job newCentroidJob = Job.getInstance(configuration, "kmeans_init_choice_" + keyword);
            newCentroidJob.setJarByClass(InitPhase.class);
            newCentroidJob.setMapperClass(DistanceMapper.class);
            newCentroidJob.setReducerClass(CentroidChooser.class);
            newCentroidJob.setMapOutputKeyClass(NullWritable.class);
            newCentroidJob.setMapOutputValueClass(DoubleVectorsWritable.class);
            newCentroidJob.setOutputKeyClass(NullWritable.class);
            newCentroidJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(newCentroidJob, pointsDataPath);
            FileOutputFormat.setOutputPath(newCentroidJob, choiceFilePath);

            fileSystem.delete(choiceFilePath, true);
            ok = newCentroidJob.waitForCompletion(true);
            if (!ok) {
                return -2;
            }

            logger.info("Init phase: ended centroid assignment ({}/{})", centroids.size() + 1, k);

            FSDataInputStream newCentroidInputStream = fileSystem.open(Path.mergePaths(choiceFilePath, new Path("/part-r-00000")));
            BufferedReader newCentroidReader = new BufferedReader(new InputStreamReader(newCentroidInputStream));

            double[] newCentroid = KMeansUtils.deserializePoint(newCentroidReader.readLine());
            centroids.add(newCentroid);
        }

        logger.info("Got k centroids: {}", centroids);
        fileSystem.delete(choiceFilePath, true);
        FSDataOutputStream chosenCentroidsOutputStream = fileSystem.create(choiceFilePath);
        String centroidsFileContents = centroids.stream().map(KMeansUtils::serializePoint).map(s -> s + ";").collect(Collectors.joining("\n"));
        chosenCentroidsOutputStream.writeChars(centroidsFileContents);
        chosenCentroidsOutputStream.hflush();
        chosenCentroidsOutputStream.close();

        // Assign to every point the nearest centroid
        Path pointsInputPath = new Path(configuration.get(KMeans.POINTS_INPUT_PATH));

        logger.info("Init phase: starting point to centroid assignment");

        Job pointAllocationJob = Job.getInstance(configuration, "kmeans_init_alloc_" + keyword);
        pointAllocationJob.setJarByClass(InitPhase.class);
        pointAllocationJob.setMapperClass(PointAllocator.class);
        pointAllocationJob.setNumReduceTasks(0);
        pointAllocationJob.setMapOutputKeyClass(Text.class);
        pointAllocationJob.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(pointAllocationJob, pointsDataPath);
        FileOutputFormat.setOutputPath(pointAllocationJob, pointsInputPath);

        fileSystem.delete(pointsInputPath, true);
        ok = pointAllocationJob.waitForCompletion(true);

        if (!ok) {
            return -3;
        }

        logger.info("Init phase: ended point to centroid assignment");

        FSDataInputStream choiceInputStream = fileSystem.open(choiceFilePath);
        BufferedReader choicesReader = new BufferedReader(new InputStreamReader(choiceInputStream));

        logger.info("Written centroids: {}", choicesReader.lines().collect(Collectors.joining("\n")));
        choiceInputStream.close();

        return 0;
    }
}

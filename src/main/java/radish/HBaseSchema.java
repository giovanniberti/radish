package radish;

public class HBaseSchema {
    public static final byte[] DATA_COLUMN_FAMILY = "data".getBytes();
    public static final byte[] ID_COLUMN = "id".getBytes();
    public static final byte[] KEYWORD_COLUMN = "keyword".getBytes();
    public static final byte[] FEATURES_COLUMN = "features".getBytes();
    public static final byte[] CLUSTER_CENTROID_COLUMN = "cluster_center".getBytes();
    public static final byte[] NEAREST_POINT_COLUMN = "nearest_point".getBytes();
}

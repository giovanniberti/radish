package radish.batch.kmeans.writables;

import org.apache.hadoop.io.ArrayWritable;

public class DoubleVectorsWritable extends ArrayWritable {
    public DoubleVectorsWritable() {
        super(DoubleArrayWritable.class);
    }

    public DoubleVectorsWritable(DoubleArrayWritable[] values) {
        super(DoubleArrayWritable.class, values);
    }
}

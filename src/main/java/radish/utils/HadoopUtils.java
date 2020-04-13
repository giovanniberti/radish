package radish.utils;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import radish.batch.kmeans.writables.DoubleArrayWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class HadoopUtils {
    public static void writeDoubleArray(DataOutput dataOutput, double[] input) throws IOException {
        dataOutput.writeInt(input.length);

        for (double d : input) {
            dataOutput.writeDouble(d);
        }
    }

    public static double[] readDoubleArray(DataInput dataInput) throws IOException {
        int length = dataInput.readInt();
        double[] result = new double[length];

        for (int i = 0; i < length; i++) {
            result[i] = dataInput.readDouble();
        }

        return result;
    }

    public static DoubleArrayWritable doubleArrayToWritable(double[] input) {
        DoubleWritable[] doubleWritables = Arrays.stream(input)
                .mapToObj(DoubleWritable::new)
                .toArray(DoubleWritable[]::new);

        DoubleArrayWritable result = new DoubleArrayWritable();
        result.set(doubleWritables);
        return result;
    }

    public static double[] writableToDoubleArray(ArrayWritable input) {
        Writable[] writables = input.get();

        return Arrays.stream(writables)
                .map(w -> ((DoubleWritable) w))
                .map(DoubleWritable::get)
                .mapToDouble(Double::doubleValue)
                .toArray();
    }
}

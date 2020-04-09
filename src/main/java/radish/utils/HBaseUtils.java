package radish.utils;

import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;

public class HBaseUtils {
    public static <T extends Writable> T readValue(byte[] rawData, Class<T> classRef) {
        DataInputStream dataStream = new DataInputStream(new ByteArrayInputStream(rawData));

        try {
            T newValue = classRef.getConstructor().newInstance();
            newValue.readFields(dataStream);

            return newValue;
        } catch (InstantiationException
                | IllegalAccessException
                | InvocationTargetException
                | NoSuchMethodException
                | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] doubleArrayToBytes(double[] input) {
        ByteBuffer bytes = ByteBuffer.allocate(input.length * Double.BYTES);
        bytes.asDoubleBuffer().put(input);

        return bytes.array();
    }

    public static double[] byteArrayToDoubles(byte[] raw) {
        ByteBuffer bytes = ByteBuffer.wrap(raw);

        int length = bytes.array().length / Double.BYTES;
        double[] output = new double[length];
        bytes.asDoubleBuffer().get(output);

        return output;
    }
}

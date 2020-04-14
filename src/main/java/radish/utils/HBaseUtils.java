package radish.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.io.Writable;
import radish.HBaseSchema;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Arrays;

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
        for (int i = 0; i < output.length; i++) {
            output[i] = bytes.getDouble(i);
        }

        return output;
    }

    public static Scan getScanFilteredByKeyword(String keyword) {
        Filter keywordFilter = new FilterBase() {
            @Override
            public ReturnCode filterCell(Cell c) {
                byte[] family = c.getFamilyArray();
                byte[] qualifier = c.getQualifierArray();

                boolean familyMatch = Arrays.equals(family, HBaseSchema.DATA_COLUMN_FAMILY);
                boolean qualifierMatch = Arrays.equals(qualifier, HBaseSchema.KEYWORD_COLUMN);
                boolean keywordMatch = Arrays.equals(c.getValueArray(), keyword.getBytes());

                if (familyMatch && qualifierMatch && keywordMatch) {
                    return ReturnCode.INCLUDE_AND_SEEK_NEXT_ROW;
                } else {
                    return ReturnCode.NEXT_COL;
                }
            }
        };

        Scan filteredScan = new Scan();
        filteredScan.setFilter(keywordFilter);

        return filteredScan;
    }
}

package radish.apiserver;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class HBaseUtils {
    public static double[] byteArrayToDoubles(byte[] raw) {
        ByteBuffer bytes = ByteBuffer.wrap(raw);

        int length = bytes.array().length / Double.BYTES;
        double[] output = new double[length];
        bytes.asDoubleBuffer().get(output);

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
        filteredScan.setOneRowLimit();
        filteredScan.setFilter(keywordFilter);

        return filteredScan;
    }
}

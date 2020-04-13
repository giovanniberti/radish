package radish.batch.kmeans;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

public class KMeansUtils {
    public static String serializePoint(double[] point) {
        return Arrays.stream(point)
                .mapToObj(String::valueOf)
                .collect(Collectors.joining(" "));
    }

    public static double[] deserializePoint(String serialized) {
        NumberFormat numberFormat = DecimalFormat.getInstance(Locale.getDefault());
        DecimalFormatSymbols customSymbol = new DecimalFormatSymbols();
        customSymbol.setDecimalSeparator('.');
        ((DecimalFormat) numberFormat).setDecimalFormatSymbols(customSymbol);
        return Arrays.stream(serialized.split(" "))
                .map(s -> s.replaceAll("[^ -~]", ""))
                .mapToDouble(s -> {
                    try {
                        return numberFormat.parse(s).doubleValue();
                    } catch (ParseException e) {
                        throw new RuntimeException("failed to parse string: \"" + s + "\", " + s.equals("0.0"), e);
                    }
                })
                .toArray();
    }
}

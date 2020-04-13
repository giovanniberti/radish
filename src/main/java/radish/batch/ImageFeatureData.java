package radish.batch;

import org.apache.hadoop.io.Writable;
import radish.utils.HadoopUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ImageFeatureData implements Writable {
    public String imageId;
    public double[] features;

    public ImageFeatureData() {}

    public ImageFeatureData(String imageId, double[] features) {
        this.imageId = imageId;
        this.features = features;
    }

    public ImageFeatureData(ImageFeatureData other) {
        this(other.imageId, other.features.clone());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(imageId);
        HadoopUtils.writeDoubleArray(dataOutput, features);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        imageId = dataInput.readUTF();
        features = HadoopUtils.readDoubleArray(dataInput);
    }

    public static ImageFeatureData read(DataInput dataInput) throws IOException {
        ImageFeatureData imageFeatureData = new ImageFeatureData();
        imageFeatureData.readFields(dataInput);
        return imageFeatureData;
    }

    @Override
    public String toString() {
        return "ImageFeatureData{" +
                "imageId='" + imageId + '\'' +
                '}';
    }
}

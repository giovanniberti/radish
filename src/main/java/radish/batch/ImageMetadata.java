package radish.batch;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class ImageMetadata implements Writable {
    public String imageId;
    public String imagePath;

    public ImageMetadata() {}

    public ImageMetadata(String imageId, String imagePath) {
        this.imageId = imageId;
        this.imagePath = imagePath;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(imageId);
        dataOutput.writeUTF(imagePath);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        imageId = dataInput.readUTF();
        imagePath = dataInput.readUTF();
    }

    public static ImageMetadata read(DataInput dataInput) throws IOException {
        ImageMetadata imageMetadata = new ImageMetadata();
        imageMetadata.readFields(dataInput);
        return imageMetadata;
    }
}

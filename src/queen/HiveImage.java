package queen;

import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.ImageState;

public class HiveImage {

  private final Image image;

  public HiveImage(Image image) {
    this.image = image;
  }

  public String getId() {
    return image.getImageId();
  }

  public boolean isAvailable() {
    return getState() == ImageState.Available;
  }

  public ImageState getState() {
    return ImageState.fromValue(image.getState());
  }

  @Override
  public String toString() {
    return getId();
  }

}

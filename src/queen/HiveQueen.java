package queen;

import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.normalize;
import static ox.util.Utils.only;

import java.time.Duration;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateImageRequest;
import com.amazonaws.services.ec2.model.CreateImageResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;

import ox.Config;
import ox.Log;
import ox.x.XList;
import ox.x.XOptional;

public class HiveQueen {

  private final AmazonEC2 ec2;

  public HiveQueen(Config config) {
    this(config.get("hivequeen.key"), config.get("hivequeen.secret"));
  }

  public HiveQueen(String key, String secret) {
    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret));
    ec2 = AmazonEC2ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();
  }

  public HiveInstance getInstance(String instanceId) {
    return getInstances(new DescribeInstancesRequest().withInstanceIds(instanceId)).only().get();
  }

  public XList<HiveInstance> getInstances() {
    return getInstances(new DescribeInstancesRequest())
        .filter(i -> !i.isTerminated());
  }

  private XList<HiveInstance> getInstances(DescribeInstancesRequest request) {
    final XList<HiveInstance> ret = XList.create();
    ec2.describeInstances(request).getReservations().forEach(reservation -> {
      reservation.getInstances().forEach(i -> {
        ret.add(new HiveInstance(this, i));
      });
    });
    return ret;
  }

  public HiveInstance cloneInstance(String instanceId, boolean reboot, boolean useExistingImageIfAvailable,
      boolean copyTags) {
    Log.info("Cloning instance: " + instanceId);

    HiveInstance existingInstance = getInstance(instanceId);
    HiveInstance ret = cloneInternal(existingInstance, reboot, useExistingImageIfAvailable);

    if (copyTags) {
      Log.debug("Copying tags.");
      existingInstance.getTags().forEach((k, v) -> {
        if (!k.equalsIgnoreCase("Name")) {
          ret.withTag(k, v);
        }
      });
    }

    return ret;
  }

  private HiveInstance cloneInternal(HiveInstance existingInstance, boolean reboot, boolean useExistingImageIfAvailable) {
    String cloneName = existingInstance.getName() + " (Cloned)";

    if (useExistingImageIfAvailable) {
      XOptional<HiveImage> existingImage = getImageByName(existingInstance.getId());
      if (existingImage.isPresent()) {
        return launchInstanceFromImage(cloneName, existingInstance.getType(), existingImage.get().getId());
      }
    }

    Log.debug("Creating Image...");
    CreateImageResult result = ec2.createImage(new CreateImageRequest()
        .withInstanceId(existingInstance.getId())
        .withName(existingInstance.getId())
        .withNoReboot(!reboot));

    final String imageId = result.getImageId();

    Await.every(Duration.ofSeconds(5))
        .timeout(Duration.ofMinutes(15))
        .verbose("Image Creation")
        .await(() -> getImage(imageId).isAvailable());

    return launchInstanceFromImage(cloneName, existingInstance.getType(), imageId);
  }

  public HiveInstance launchInstanceFromImage(String instanceName, InstanceType type, String imageId) {
    imageId = checkNotEmpty(normalize(imageId));

    Log.debug("Launching instance.");
    Reservation reservation = ec2.runInstances(new RunInstancesRequest()
        .withMinCount(1).withMaxCount(1)
        .withInstanceType(type)
        .withImageId(imageId)
        .withMonitoring(true)).getReservation();

    HiveInstance ret = new HiveInstance(this, only(reservation.getInstances()));
    Log.debug("New Instance Created!  id = " + ret.getId());

    ret.withTag("Name", instanceName);

    return ret;
  }

  public HiveImage getImage(String imageId) {
    Image ret = only(ec2.describeImages(new DescribeImagesRequest().withImageIds(imageId)).getImages());
    return new HiveImage(ret);
  }

  public XOptional<HiveImage> getImageByName(String imageName) {
    DescribeImagesRequest request = new DescribeImagesRequest()
        .withFilters(new Filter().withName("name").withValues(imageName));
    List<Image> images = ec2.describeImages(request).getImages();
    return XList.create(images).map(HiveImage::new).only();
  }

  protected AmazonEC2 getEC2() {
    return ec2;
  }

  public static void main(String[] args) {
    HiveQueen queen = new HiveQueen(Config.load("ender"));

    queen.cloneInstance("i-08da31479155e2dbe", false, true, true);
    // HiveInstance newInstance = queen.launchInstanceFromAMI(InstanceType.T3Micro, "ami-09dd4b5b94e8f714a");
    // Log.debug(newInstance.getState());
  }

}

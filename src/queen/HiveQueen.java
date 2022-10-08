package queen;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;

import ox.Config;
import ox.x.XList;

public class HiveQueen {

  private final AmazonEC2 ec2;

  public HiveQueen(Config config) {
    this(config.get("hivequeen.key"), config.get("hivequeen.secret"));
  }

  public HiveQueen(String key, String secret) {
    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret));
    ec2 = AmazonEC2ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();
    // ec2.createTags(createTagsRequest)
  }

  public XList<HiveInstance> getInstances() {
    final XList<HiveInstance> ret = XList.create();
    ec2.describeInstances().getReservations().forEach(reservation -> {
      reservation.getInstances().forEach(i -> {
        ret.add(new HiveInstance(this, i));
      });
    });
    return ret.filter(i -> !i.isTerminated());
  }

  protected AmazonEC2 getEC2() {
    return ec2;
  }

  public static void main(String[] args) {
    HiveQueen queen = new HiveQueen(Config.load("ender"));
    queen.getInstances().forEach(instance -> {
      instance.getTags().log();
    });
  }

}

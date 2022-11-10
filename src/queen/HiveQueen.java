package queen;

import static com.google.common.base.Preconditions.checkState;
import static ox.util.Utils.checkNotEmpty;
import static ox.util.Utils.format;
import static ox.util.Utils.normalize;
import static ox.util.Utils.only;
import static ox.util.Utils.sleep;

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
import com.amazonaws.services.route53.AmazonRoute53;
import com.amazonaws.services.route53.AmazonRoute53ClientBuilder;
import com.amazonaws.services.route53.model.Change;
import com.amazonaws.services.route53.model.ChangeAction;
import com.amazonaws.services.route53.model.ChangeBatch;
import com.amazonaws.services.route53.model.ChangeResourceRecordSetsRequest;
import com.amazonaws.services.route53.model.ChangeResourceRecordSetsResult;
import com.amazonaws.services.route53.model.ChangeStatus;
import com.amazonaws.services.route53.model.GetChangeRequest;
import com.amazonaws.services.route53.model.HostedZone;
import com.amazonaws.services.route53.model.ListHostedZonesRequest;
import com.amazonaws.services.route53.model.ListResourceRecordSetsRequest;
import com.amazonaws.services.route53.model.RRType;
import com.amazonaws.services.route53.model.ResourceRecord;
import com.amazonaws.services.route53.model.ResourceRecordSet;
import com.google.common.base.Splitter;

import ox.Config;
import ox.Log;
import ox.x.XList;
import ox.x.XOptional;

public class HiveQueen {

  private final AmazonEC2 ec2;
  private final AmazonRoute53 route53;

  public HiveQueen(Config config) {
    this(config.get("hivequeen.key"), config.get("hivequeen.secret"));
  }

  public HiveQueen(String key, String secret) {
    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret));

    ec2 = AmazonEC2ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();
    route53 = AmazonRoute53ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();
  }

  public HiveInstance getInstance(String instanceId) {
    return getInstances(new DescribeInstancesRequest().withInstanceIds(instanceId)).only().get();
  }

  public HiveInstance getInstanceByName(String instanceName) {
    return getInstances().filter(i -> i.getName().equals(instanceName)).only().get();
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

  private HiveInstance cloneInternal(HiveInstance existingInstance, boolean reboot,
      boolean useExistingImageIfAvailable) {
    String cloneName = existingInstance.getName() + " (Cloned)";

    if (useExistingImageIfAvailable) {
      XOptional<HiveImage> existingImage = getImageByName(existingInstance.getId());
      if (existingImage.isPresent()) {
        return launchInstanceFromImage(cloneName, existingInstance.getType(), existingImage.get().getId(), false);
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

    return launchInstanceFromImage(cloneName, existingInstance.getType(), imageId, false);
  }

  public HiveInstance launchInstanceFromImage(String instanceName, InstanceType type, String imageId, boolean awaitIp) {
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

    if (awaitIp) {
      // TODO can sometimes run into "The instance ID 'i-xyz' does not exist"
      // for now we'll add a hacky sleep
      sleep(2000);

      Await.every(Duration.ofSeconds(2))
          .timeout(Duration.ofMinutes(20))
          .verbose("Instance IP")
          .await(() -> !getInstance(ret.getId()).getIp().isEmpty());
      return getInstance(ret.getId());
    } else {
      return ret;
    }
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

  public void createDNSRecord(String key, String value, boolean awaitDNSPropagation) {
    key = checkNotEmpty(normalize(key), "Missing DNS key");
    value = checkNotEmpty(normalize(value), "Missing DNS value");

    Log.debug(format("Creating dns record: {0}={1}", key, value));

    HostedZone zone = getHostedZoneByName(getDomain(key));

    ChangeBatch change = new ChangeBatch();
    change.withChanges(new Change(ChangeAction.UPSERT,
        new ResourceRecordSet(key, RRType.A)
            .withResourceRecords(new ResourceRecord(value))
            .withTTL(Duration.ofMinutes(5).getSeconds())));

    ChangeResourceRecordSetsResult result = route53.changeResourceRecordSets(new ChangeResourceRecordSetsRequest()
        .withHostedZoneId(zone.getId())
        .withChangeBatch(change));

    if (awaitDNSPropagation) {
      final String changeId = result.getChangeInfo().getId();
      Await.every(Duration.ofSeconds(5)).timeout(Duration.ofMinutes(20)).verbose("createDNSRecord").await(() -> {
        ChangeStatus status = ChangeStatus
            .valueOf(route53.getChange(new GetChangeRequest(changeId)).getChangeInfo().getStatus());
        return status == ChangeStatus.INSYNC;
      });
      Log.debug("Record confirmed.");
    }
  }

  public void deleteDNSRecord(String key) {
    Log.debug(format("Deleting dns record: {0}", key));

    HostedZone zone = getHostedZoneByName(getDomain(key));

    ListResourceRecordSetsRequest listRequest = new ListResourceRecordSetsRequest(zone.getId())
        .withStartRecordName(key).withStartRecordType(RRType.A).withMaxItems("1");
    ResourceRecordSet record = XList.create(route53.listResourceRecordSets(listRequest)
        .getResourceRecordSets()).only().get();
    checkState(normalizeDomain(record.getName()).equals(key),
        "Could not find record: " + key + " -- first record was: " + record.getName());

    route53.changeResourceRecordSets(new ChangeResourceRecordSetsRequest()
        .withHostedZoneId(zone.getId())
        .withChangeBatch(new ChangeBatch()
            .withChanges(new Change(ChangeAction.DELETE, record))));
  }

  private String getDomain(String key) {
    List<String> m = Splitter.on('.').splitToList(key);
    return m.get(m.size() - 2) + "." + m.get(m.size() - 1);
  }

  private HostedZone getHostedZoneByName(String name) {
    return XList.create(route53.listHostedZones(new ListHostedZonesRequest()).getHostedZones())
        .filter(z -> {
          return normalizeDomain(z.getName()).equals(name);
        }).only().get();
  }

  private String normalizeDomain(String domain) {
    if (domain.endsWith(".")) {
      return domain.substring(0, domain.length() - 1);
    }
    return domain;
  }

  protected AmazonEC2 getEC2() {
    return ec2;
  }

  public static void main(String[] args) {
    HiveQueen queen = new HiveQueen(Config.load("ender"));

    // queen.getInstanceByName("cron.ender.com").changeInstanceType(InstanceType.T4gMedium);

    queen.createDNSRecord("cron.ender.com", queen.getInstanceByName("cron.ender.com").getIp(), false);

    // queen.createDNSRecord("jasontest.ender.com", "192.168.0.55", true);
    // queen.deleteDNSRecord("jasontest.ender.com");

    // queen.cloneInstance("i-08da31479155e2dbe", false, true, true);
    // HiveInstance newInstance = queen.launchInstanceFromAMI(InstanceType.T3Micro, "ami-09dd4b5b94e8f714a");
    // Log.debug(newInstance.getState());

    // queen.getInstances().forEach(i -> {
    // if (i.getTag("environment").equals("PRODUCTION")) {
    // i.removeTag("deployGroup");
    // }
    // });

    // queen.getInstances().forEach(instance -> {
    // instance.removeTag("priority");
    // });

    // queen.getInstanceByName("ender.com").withTag("deployGroup", "main");
    // queen.getInstanceByName("api.ender.com").withTag("deployGroup", "main");
    // queen.getInstanceByName("cron.ender.com").withTag("deployGroup", "main");
    // queen.getInstanceByName("chat.ender.com").withTag("deployGroup", "main");
  }

}

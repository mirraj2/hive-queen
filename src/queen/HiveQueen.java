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
import com.amazonaws.services.ec2.model.DescribeSubnetsRequest;
import com.amazonaws.services.ec2.model.DescribeVpcsRequest;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.Subnet;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancing;
import com.amazonaws.services.elasticloadbalancingv2.AmazonElasticLoadBalancingClientBuilder;
import com.amazonaws.services.elasticloadbalancingv2.model.Action;
import com.amazonaws.services.elasticloadbalancingv2.model.ActionTypeEnum;
import com.amazonaws.services.elasticloadbalancingv2.model.Certificate;
import com.amazonaws.services.elasticloadbalancingv2.model.CreateListenerRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancingv2.model.CreateTargetGroupRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.CreateTargetGroupResult;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeLoadBalancersRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.ForwardActionConfig;
import com.amazonaws.services.elasticloadbalancingv2.model.IpAddressType;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancerSchemeEnum;
import com.amazonaws.services.elasticloadbalancingv2.model.ProtocolEnum;
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroupTuple;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetTypeEnum;
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
import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;

import ox.Await;
import ox.Config;
import ox.Log;
import ox.util.Matchers;
import ox.x.XList;
import ox.x.XOptional;

public class HiveQueen {

  private final AmazonEC2 ec2;
  private final AmazonRoute53 route53;
  private final AmazonElasticLoadBalancing loadBalancing;

  public HiveQueen(Config config) {
    this(config.get("hivequeen.key"), config.get("hivequeen.secret"));
  }

  public HiveQueen(String key, String secret) {
    AWSCredentialsProvider provider = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key, secret));

    ec2 = AmazonEC2ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();
    route53 = AmazonRoute53ClientBuilder.standard().withCredentials(provider).withRegion(Regions.US_EAST_2).build();

    loadBalancing = AmazonElasticLoadBalancingClientBuilder.standard().withCredentials(provider)
        .withRegion(Regions.US_EAST_2).build();
  }

  public HiveInstance getInstance(String instanceId) {
    return getInstanceOptional(instanceId).get();
  }

  public XOptional<HiveInstance> getInstanceOptional(String instanceId) {
    return getInstances(new DescribeInstancesRequest().withInstanceIds(instanceId)).only();
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
          .await(() -> {
            return getInstanceOptional(ret.getId())
                .compute(instance -> !instance.getIp().isEmpty(), false);
          });
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

  public void createARecord(String key, String value, boolean awaitDNSPropagation) {
    createDNSRecord(key, value, awaitDNSPropagation, RRType.A);
  }

  public void createDNSRecord(String key, String value, boolean awaitDNSPropagation) {
    boolean isIP = Matchers.javaDigit().or(CharMatcher.is('.')).matchesAllOf(value);
    createDNSRecord(key, value, awaitDNSPropagation, isIP ? RRType.A : RRType.CNAME);
  }

  private void createDNSRecord(String key, String value, boolean awaitDNSPropagation, RRType type) {
    key = checkNotEmpty(normalize(key), "Missing DNS key");
    value = checkNotEmpty(normalize(value), "Missing DNS value");

    Log.debug(format("Creating dns record: {0}={1}", key, value));

    HostedZone zone = getHostedZoneByName(getDomain(key));

    ChangeBatch change = new ChangeBatch();
    change.withChanges(new Change(ChangeAction.UPSERT,
        new ResourceRecordSet(key, type)
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

  public String createTargetGroup(String name, String vpcId, String healthCheckPath) {
    CreateTargetGroupResult result = loadBalancing.createTargetGroup(new CreateTargetGroupRequest()
        .withTargetType(TargetTypeEnum.Instance)
        .withName(name)
        .withVpcId(vpcId)
        .withProtocol(ProtocolEnum.HTTPS)
        .withPort(443)
        .withHealthCheckEnabled(true)
        .withHealthCheckProtocol(ProtocolEnum.HTTPS)
        .withHealthCheckPath(healthCheckPath));

    return only(result.getTargetGroups()).getTargetGroupArn();
  }

  public LoadBalancer createLoadBalancer(String name, String vpcId) {
    XList<Subnet> subnets = getSubnets(vpcId);

    CreateLoadBalancerResult result = loadBalancing.createLoadBalancer(new CreateLoadBalancerRequest()
        .withName(name)
        .withScheme(LoadBalancerSchemeEnum.InternetFacing)
        .withIpAddressType(IpAddressType.Dualstack)
        .withSubnets(subnets.map(s -> s.getSubnetId())));

    LoadBalancer ret = only(result.getLoadBalancers());

    checkState(!ret.getDNSName().isEmpty());

    return ret;
  }

  public void addLoadBalancerListener(String loadBalancerId, String targetGroupId, String certificateId) {
    loadBalancing.createListener(new CreateListenerRequest()
        .withLoadBalancerArn(loadBalancerId)
        .withProtocol(ProtocolEnum.HTTPS)
        .withPort(443)
        .withDefaultActions(new Action()
            .withType(ActionTypeEnum.Forward)
            .withForwardConfig(new ForwardActionConfig()
                .withTargetGroups(new TargetGroupTuple()
                    .withTargetGroupArn(targetGroupId)
                    .withWeight(1))))
        .withCertificates(new Certificate()
            .withCertificateArn(certificateId)));
  }

  public void registerTargets(String targetGroupId, XList<String> instanceIds) {
    loadBalancing.registerTargets(new RegisterTargetsRequest()
        .withTargetGroupArn(targetGroupId)
        .withTargets(instanceIds.map(instanceId -> new TargetDescription().withId(instanceId).withPort(443))));
  }

  public HiveVPC getVPC(String name) {
    return XList.create(ec2.describeVpcs(new DescribeVpcsRequest()).getVpcs())
        .map(HiveVPC::new)
        .filter(vpc -> vpc.getName().equals(name))
        .only().get();
  }

  public XList<Subnet> getSubnets(String vpcId) {
    return XList.create(ec2.describeSubnets(new DescribeSubnetsRequest()).getSubnets())
        .filter(s -> s.getVpcId().equals(vpcId));
  }

  public XList<LoadBalancer> getLoadBalancers() {
    return XList.create(loadBalancing.describeLoadBalancers(new DescribeLoadBalancersRequest()).getLoadBalancers());
  }

  protected AmazonEC2 getEC2() {
    return ec2;
  }

  public static void main(String[] args) {
    Config config = Config.load("ender");
    HiveQueen queen = new HiveQueen(config);

    queen.getInstances().filter(i -> i.getName().contains("node")).forEach(i -> i.withTag("branch", "production"));

    // String vpcId = queen.getVPC("Ender Default VPC").getId();

    // queen.createTargetGroup("web-server", queen.getVPC("Ender Default VPC").getId(),
    // "/serverStatus?password=" + config.get("server.status.pw"));

    // queen.foo();
    // queen.createLoadBalancer("web-server", vpcId);
    // queen.addLoadBalancerListener(
    // "arn:aws:elasticloadbalancing:us-east-2:646212457017:loadbalancer/app/web-server/5b782801ebb1a79c",
    // "arn:aws:elasticloadbalancing:us-east-2:646212457017:targetgroup/web-server/872a431a6aca6c9f",
    // "arn:aws:acm:us-east-2:646212457017:certificate/06d6b931-e319-4ee1-8bc6-82242bd1d82a");

    // queen.registerTargets("arn:aws:elasticloadbalancing:us-east-2:646212457017:targetgroup/web-server/872a431a6aca6c9f",
    // XList.of("i-044c2030d5656dad2", "i-0f6eefec7220bcc3a"));

    // queen.createDNSRecord("jasontest.ender.com", "web-server-1562555251.us-east-2.elb.amazonaws.com", true);

    // queen.getInstanceByName("cron.ender.com").changeInstanceType(InstanceType.T4gMedium);

    // queen.createDNSRecord("qa13.ender.com", queen.getInstanceByName("qa13.ender.com").getIp(), false);

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

    Log.debug("Done.");
  }

}

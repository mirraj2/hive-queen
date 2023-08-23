package queen;

import static ox.util.Utils.attempt;
import static ox.util.Utils.normalize;

import java.time.Duration;

import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.ModifyInstanceAttributeRequest;
import com.amazonaws.services.ec2.model.RebootInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

import ox.Await;
import ox.Await.AwaitTimeoutException;
import ox.Json;
import ox.Log;
import ox.x.XList;

public class HiveInstance {

  private final HiveQueen queen;
  private final Instance instance;

  public HiveInstance(HiveQueen queen, Instance instance) {
    this.queen = queen;
    this.instance = instance;
  }

  public String getId() {
    return instance.getInstanceId();
  }

  public String getIp() {
    return normalize(instance.getPublicIpAddress());
  }

  public String getInternalIp() {
    return normalize(instance.getPrivateIpAddress());
  }

  public boolean isTerminated() {
    return getState() == InstanceStateName.Terminated;
  }

  public boolean isRunning() {
    return getState() == InstanceStateName.Running;
  }

  public boolean isStopped() {
    return getState() == InstanceStateName.Stopped;
  }

  public void awaitRunning() {
    awaitState(InstanceStateName.Running);
  }

  public void awaitState(InstanceStateName state) {
    awaitState(state, Duration.ofMinutes(10));
  }

  public void awaitState(InstanceStateName state, Duration timeout) {
    Await.every(Duration.ofSeconds(1))
        .timeout(timeout)
        .verbose("Instance " + state)
        .await(() -> queen.getInstance(getId()).getState() == state);
  }

  public void awaitIp() {
    Await.every(Duration.ofSeconds(2))
        .timeout(Duration.ofMinutes(20))
        .verbose("Instance IP")
        .await(() -> {
          return queen.getInstanceOptional(getId())
              .compute(instance -> !instance.getIp().isEmpty(), false);
        });
  }

  public void reboot() {
    queen.getEC2().rebootInstances(new RebootInstancesRequest(XList.of(getId())));

    try {
      Await.every(Duration.ofSeconds(1))
          .timeout(Duration.ofSeconds(5))
          .verbose("Instance Shutting Down")
          .await(() -> !queen.getInstance(getId()).isRunning());
    } catch (AwaitTimeoutException e) {
      throw new RuntimeException("There was a problem shutting this instance down!");
    }
  }

  public void hardReboot() {
    stop();
    start();
    awaitIp();
  }

  /**
   * Routes the given domain to this instance.
   */
  public void routeDomainToInstance(String domain) {
    queen.createARecord(domain, getIp(), false);
  }

  /**
   * Attempts to stop normally. If the instance isn't stopped after 1 minute, this will attempt to force-stop the
   * instance.
   */
  public void stop() {
    queen.getEC2().stopInstances(new StopInstancesRequest(XList.of(getId())));
    try {
      awaitState(InstanceStateName.Stopped, Duration.ofMinutes(1));
    } catch (AwaitTimeoutException e) {
      Log.debug("Stopping with force.");
      queen.getEC2().stopInstances(new StopInstancesRequest(XList.of(getId())).withForce(true));
      awaitState(InstanceStateName.Stopped, Duration.ofMinutes(9));
    }
  }

  public void start() {
    queen.getEC2().startInstances(new StartInstancesRequest(XList.of(getId())));
  }

  public void terminate() {
    queen.getEC2().terminateInstances(new TerminateInstancesRequest(XList.of(getId())));
  }

  public void changeInstanceType(InstanceType type) {
    if (getType() == type) {
      return;
    }
    stop();
    queen.getEC2().modifyInstanceAttribute(
        new ModifyInstanceAttributeRequest()
            .withInstanceId(getId())
            .withInstanceType(type.toString()));
    queen.getEC2().startInstances(new StartInstancesRequest(XList.of(getId())));
    awaitIp();
  }

  public InstanceStateName getState() {
    return InstanceStateName.fromValue(instance.getState().getName());
  }

  public InstanceType getType() {
    return InstanceType.fromValue(instance.getInstanceType());
  }

  public HiveInstance withTag(String key, Object value) {
    String s = value == null ? "" : normalize(value.toString());
    if (s.isEmpty()) {
      removeTag(key);
    } else {
      Tag tag = new Tag(key, s);
      attempt(() -> {
        queen.getEC2().createTags(new CreateTagsRequest()
            .withResources(instance.getInstanceId())
            .withTags(tag));
        return true;
      }, 10, 1000);
      instance.withTags(tag);
    }
    return this;
  }

  public HiveInstance removeTag(String key) {
    queen.getEC2()
        .deleteTags(new DeleteTagsRequest()
            .withResources(instance.getInstanceId())
            .withTags(new Tag(key)));

    return this;
  }

  public Json getTags() {
    Json ret = Json.object();
    instance.getTags().forEach(tag -> {
      ret.with(tag.getKey(), tag.getValue());
    });
    return ret;
  }

  public String getTag(String key) {
    return normalize(getTags().get(key));
  }

  public String getName() {
    return getTag("Name");
  }

  @Override
  public String toString() {
    String name = getName();
    return name.isEmpty() ? getId() : name;
  }

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HiveInstance)) {
      return false;
    }
    return getId().equals(((HiveInstance) obj).getId());
  }

}

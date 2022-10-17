package queen;

import static ox.util.Utils.normalize;

import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DeleteTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceStateName;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Tag;

import ox.Json;

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

  public boolean isTerminated() {
    return getState() == InstanceStateName.Terminated;
  }

  public boolean isRunning() {
    return getState() == InstanceStateName.Running;
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
      queen.getEC2()
          .createTags(new CreateTagsRequest()
              .withResources(instance.getInstanceId())
              .withTags(tag));
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

}

package queen;

import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.Vpc;

public class HiveVPC {

  private final Vpc vpc;

  public HiveVPC(Vpc vpc) {
    this.vpc = vpc;
  }

  public String getId() {
    return vpc.getVpcId();
  }

  public String getName() {
    return getTag("Name");
  }

  public String getTag(String key) {
    for (Tag tag : vpc.getTags()) {
      if (tag.getKey().equalsIgnoreCase(key)) {
        return tag.getValue();
      }
    }
    return "";
  }

  @Override
  public String toString() {
    return getName();
  }

}

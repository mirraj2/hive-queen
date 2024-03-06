package queen;

import java.time.Duration;

import com.amazonaws.services.elasticloadbalancingv2.model.DeregisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetGroupsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.DescribeTargetHealthRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.LoadBalancer;
import com.amazonaws.services.elasticloadbalancingv2.model.RegisterTargetsRequest;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetGroup;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthDescription;
import com.amazonaws.services.elasticloadbalancingv2.model.TargetHealthStateEnum;

import ox.Await;
import ox.x.XList;
import ox.x.XMap;

public class HiveLoadBalancer {

  private final HiveQueen queen;
  private final LoadBalancer loadBalancer;

  private TargetGroup cachedTargetGroup = null;

  protected HiveLoadBalancer(HiveQueen queen, LoadBalancer loadBalancer) {
    this.queen = queen;
    this.loadBalancer = loadBalancer;
  }

  public XList<TargetGroup> getTargetGroups() {
    return XList.create(queen.getLoadBalancing()
        .describeTargetGroups(new DescribeTargetGroupsRequest().withLoadBalancerArn(loadBalancer.getLoadBalancerArn()))
        .getTargetGroups());
  }

  public XList<HiveInstance> getTargets() {
    return getTargetsWithHealth().keySet().toList();
  }

  public XMap<HiveInstance, TargetHealthStateEnum> getTargetsWithHealth() {
    XList<TargetHealthDescription> descriptions = XList.create(queen.getLoadBalancing()
        .describeTargetHealth(
            new DescribeTargetHealthRequest().withTargetGroupArn(getTargetGroup().getTargetGroupArn()))
        .getTargetHealthDescriptions());
    return descriptions.toMap(d -> queen.getInstance(d.getTarget().getId()),
        d -> TargetHealthStateEnum.fromValue(d.getTargetHealth().getState()));
  }

  public TargetHealthStateEnum getTargetHealth(HiveInstance instance) {
    return getTargetsWithHealth().get(instance);
  }

  public void deregister(HiveInstance instance, boolean awaitFullyDrained) {
    queen.getLoadBalancing()
        .deregisterTargets(new DeregisterTargetsRequest().withTargetGroupArn(getTargetGroup().getTargetGroupArn())
            .withTargets(new TargetDescription().withId(instance.getId())));
    if (awaitFullyDrained) {
      Await.every(Duration.ofSeconds(2)).verbose("Deregistering taget").timeout(Duration.ofHours(1)).await(() -> {
        TargetHealthStateEnum state = getTargetHealth(instance);
        return state != TargetHealthStateEnum.Draining && state != TargetHealthStateEnum.Healthy;
      });
    }
  }

  public void register(HiveInstance instance, boolean awaitHealthy) {
    queen.getLoadBalancing()
        .registerTargets(new RegisterTargetsRequest().withTargetGroupArn(getTargetGroup().getTargetGroupArn())
            .withTargets(new TargetDescription().withId(instance.getId())));
    if (awaitHealthy) {
      Await.every(Duration.ofSeconds(2)).verbose("Registering target").timeout(Duration.ofHours(1)).await(() -> {
        return getTargetHealth(instance) == TargetHealthStateEnum.Healthy;
      });
    }
  }

  private TargetGroup getTargetGroup() {
    if (cachedTargetGroup == null) {
      cachedTargetGroup = getTargetGroups().only().get();
    }
    return cachedTargetGroup;
  }

}

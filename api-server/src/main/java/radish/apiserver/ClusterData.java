package radish.apiserver;

import lombok.Data;

@Data
public class ClusterData {
    private final double[] clusterCentroid;
    private final double[] nearestMember;
}

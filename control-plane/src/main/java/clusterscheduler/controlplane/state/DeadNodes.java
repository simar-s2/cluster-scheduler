package clusterscheduler.controlplane.state;

import java.util.List;

/** Nodes just declared dead, and the jobs that went back to the queue because of it. */
public record DeadNodes(List<String> nodeIds, List<String> requeuedJobIds) {
    public boolean isEmpty() {
        return nodeIds.isEmpty();
    }
}

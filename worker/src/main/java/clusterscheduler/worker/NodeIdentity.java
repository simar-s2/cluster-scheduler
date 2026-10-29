package clusterscheduler.worker;

/**
 * The node ID this worker currently holds. It is null before the first
 * registration and changes every time the worker registers again.
 */
final class NodeIdentity {
    private volatile String nodeId;

    String current() {
        return nodeId;
    }

    void set(String newNodeId) {
        nodeId = newNodeId;
    }

    void clear() {
        nodeId = null;
    }
}

package clusterscheduler.controlplane.delivery;

import clusterscheduler.proto.WorkerGrpc;
import clusterscheduler.proto.WorkerGrpc.WorkerBlockingStub;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** One gRPC connection per worker node, opened the first time it is needed. */
final class WorkerChannels implements AutoCloseable {
    private final Map<String, ManagedChannel> channelsByNodeId = new ConcurrentHashMap<>();

    WorkerBlockingStub stubFor(String nodeId, String address) {
        ManagedChannel channel = channelsByNodeId.computeIfAbsent(
                nodeId, id -> ManagedChannelBuilder.forTarget(address).usePlaintext().build());
        return WorkerGrpc.newBlockingStub(channel);
    }

    /** Called when a node is declared dead, so its connection does not linger. */
    void close(String nodeId) {
        ManagedChannel channel = channelsByNodeId.remove(nodeId);
        if (channel != null) {
            channel.shutdownNow();
        }
    }

    @Override
    public void close() {
        channelsByNodeId.values().forEach(ManagedChannel::shutdownNow);
        channelsByNodeId.clear();
    }
}

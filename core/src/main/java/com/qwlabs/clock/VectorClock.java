package com.qwlabs.clock;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.ThreadSafe;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@ThreadSafe
public class VectorClock {
    private final NodeIds nodeIds;

    private final Clocks clocks;

    public VectorClock(NodeIds nodeIds) {
        Preconditions.checkNotNull(nodeIds, "Node Ids must not be null.");
        this.nodeIds = nodeIds;
        this.clocks = new Clocks();
    }

    public VectorClock(NodeIds nodeIds, Map<String, Integer> versions) {
        Preconditions.checkNotNull(nodeIds, "Node Ids must not be null.");
        this.nodeIds = nodeIds;
        if (versions == null || versions.isEmpty()) {
            this.clocks = new Clocks();
            return;
        }
        Clocks clocks = new Clocks(versions.size());
        int index = 0;
        for (Map.Entry<String, Integer> entry : versions.entrySet()) {
            clocks.set(nodeIds.index(entry.getKey()), entry.getValue(), index);
            index++;
        }
        this.clocks = clocks;
    }

    public VectorClock(VectorClock vectorClock) {
        this.nodeIds = vectorClock.nodeIds;
        this.clocks = new Clocks(vectorClock.clocks);
    }

    public synchronized void tick(String nodeId) {
        this.clocks.tick(this.nodeIds.index(nodeId));
    }

    public synchronized Map<String, Integer> clocks() {
        Map<String, Integer> nodeClocks = new HashMap<>();
        for (Clocks.Entry entry : this.clocks) {
            nodeClocks.put(this.nodeIds.nodeId(entry.getNodeIndex()), entry.getClock());
        }
        return nodeClocks;
    }

    public synchronized void merge(VectorClock vectorClock) {
        this.clocks.merge(vectorClock.clocks);
    }

    public synchronized void remove(String nodeId) {
        this.clocks.remove(this.nodeIds.index(nodeId));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VectorClock that = (VectorClock) o;
        return Objects.equals(clocks, that.clocks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clocks);
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (Clocks.Entry entry : this.clocks) {
            String nodeId = this.nodeIds.nodeId(entry.getNodeIndex());
            int clock = entry.getClock();
            builder.append("{").append(nodeId).append(":").append(clock).append("}");
        }
        builder.append("]");
        return builder.toString();
    }
}

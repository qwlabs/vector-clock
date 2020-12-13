package com.qwlabs.clock;

import com.google.common.base.Preconditions;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class VectorClocks<K> {
    private final Map<K, VectorClock> vectorClocks = new ConcurrentHashMap<>();

    private final NodeIds nodeIds;

    public VectorClocks(NodeIds nodeIds) {
        this.nodeIds = nodeIds;
    }

    @NonNull
    public VectorClock tick(@NonNull K key, @NonNull String nodeId) {
        Preconditions.checkNotNull(key, "Key must not be null");
        Preconditions.checkNotNull(nodeId, "Node ID must not be null");
        if (this.vectorClocks.get(key) == null) {
            this.vectorClocks.put(key, new VectorClock(this.nodeIds));
        }
        this.vectorClocks.get(key).tick(nodeId);
        return this.vectorClocks.get(key);
    }

    @NonNull
    public VectorClock tickClone(@NonNull K key, @NonNull String nodeId) {
        Preconditions.checkNotNull(key, "Key must not be null");
        Preconditions.checkNotNull(nodeId, "Node ID must not be null");
        return new VectorClock(tick(key, nodeId));
    }

    @NonNull
    public VectorClock putClone(@NonNull K key) {
        Preconditions.checkNotNull(key, "Key must not be null");
        return new VectorClock(put(key));
    }

    @NonNull
    public VectorClock put(@NonNull K key) {
        Preconditions.checkNotNull(key, "Key must not be null");
        VectorClock vectorClock = this.vectorClocks.get(key);
        if (vectorClock == null) {
            vectorClock = new VectorClock(this.nodeIds);
            this.vectorClocks.put(key, vectorClock);
        }
        return vectorClock;
    }

    public void put(@NonNull K key, @NonNull VectorClock vectorClock) {
        Preconditions.checkNotNull(key, "Key must not be null");
        Preconditions.checkNotNull(vectorClock, "Vector clock must not be null");
        this.vectorClocks.put(key, vectorClock);
    }

    public void remove(@NonNull K key) {
        Preconditions.checkNotNull(key, "Key must not be null");
        this.vectorClocks.remove(key);
    }

    public Set<K> nodes() {
        return this.vectorClocks.keySet();
    }

    public String toString() {
        return "Vector clock: " + this.vectorClocks.toString();
    }
}

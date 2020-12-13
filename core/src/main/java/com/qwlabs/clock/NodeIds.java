package com.qwlabs.clock;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeIds {
    private final AtomicInteger maxIndex = new AtomicInteger(0);

    private final ConcurrentHashMap<String, Integer> nodeIndexMapping = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<Integer, String> indexNodeMapping = new ConcurrentHashMap<>();

    public int index(String nodeId) {
        Integer index = this.nodeIndexMapping.get(nodeId);
        if (index != null) {
            return index;
        }
        int nextIndex = this.maxIndex.getAndIncrement();
        index = this.nodeIndexMapping.putIfAbsent(nodeId, nextIndex);
        if (index == null) {
            index = nextIndex;
        }
        this.indexNodeMapping.put(index, nodeId);
        return index;
    }

    public String nodeId(int index) {
        while (true) {
            String nodeId = this.indexNodeMapping.get(index);
            if (nodeId != null) {
                return nodeId;
            }
        }
    }
}

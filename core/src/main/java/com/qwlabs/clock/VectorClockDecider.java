package com.qwlabs.clock;

import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class VectorClockDecider {

    public boolean before(@NonNull VectorClock requestVectorClock, @NonNull VectorClock localVectorClock) {
        Map<String, Integer> requestClocks = requestVectorClock.clocks();
        Map<String, Integer> localClocks = localVectorClock.clocks();
        Set<String> nodeIds = new HashSet<>(requestClocks.size() + localClocks.size());
        nodeIds.addAll(requestClocks.keySet());
        nodeIds.addAll(localClocks.keySet());
        boolean before = false;
        for (String nodeId : nodeIds) {
            int requestClock = Objects.<Integer>requireNonNullElse(requestClocks.get(nodeId), 0);
            int localClock = Objects.<Integer>requireNonNullElse(localClocks.get(nodeId), 0);
            if (requestClock > localClock) {
                return false;
            }
            if (requestClock < localClock) {
                before = true;
            }
        }
        return before;
    }

    public <T> void decide(@NonNull VectorClock requestVectorClock,
                           @NonNull VectorClock localVectorClock,
                           @NonNull VectorClocks<T> vectorClocks,
                           @NonNull String nodeId,
                           @NonNull T key,
                           @NonNull Operator operator) {
        if (before(requestVectorClock, localVectorClock) || requestVectorClock.equals(localVectorClock)) {
            operator.ignore();
            return;
        }
        if (before(localVectorClock, requestVectorClock)) {
            vectorClocks.put(key, requestVectorClock);
            operator.accept();
        } else {
            VectorClock newVectorClock = new VectorClock(localVectorClock);
            newVectorClock.merge(requestVectorClock);
            newVectorClock.remove(nodeId);
            vectorClocks.put(key, newVectorClock);
            operator.conflict();
        }
    }

    public static abstract class Operator {

        public void ignore() {
        }

        public void accept() {
        }

        public void conflict() {
        }
    }
}

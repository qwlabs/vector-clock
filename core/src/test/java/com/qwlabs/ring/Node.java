package com.qwlabs.ring;

import com.google.common.collect.Maps;
import com.qwlabs.clock.NodeIds;
import com.qwlabs.clock.VectorClock;
import com.qwlabs.clock.VectorClockDecider;
import com.qwlabs.clock.VectorClocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Node {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
    private final String name;
    private final NodeIds nodeIds;
    private final VectorClocks<String> vectorClocks;
    private final Map<String, TimestampedObject<?>> data;
    private final VectorClockDecider decider;
    private volatile boolean running;

    public Node(String name) {
        this.name = name;
        this.nodeIds = new NodeIds();
        this.vectorClocks = new VectorClocks<>(nodeIds);
        this.data = Maps.newHashMap();
        this.decider = new VectorClockDecider();
        this.running = true;
    }

    public void stop() {
        this.running = false;
        LOGGER.error("node {} stopped.", name);
    }

    public void start() {
        this.running = true;
        LOGGER.error("node {} started.", name);
    }

    public boolean isRunning() {
        return running;
    }

    public boolean isStopped(){
        return !isRunning();
    }

    public String getName() {
        return name;
    }

    public VectorClocks<String> getVectorClocks() {
        return vectorClocks;
    }

    public Map<String, TimestampedObject<?>> getData() {
        return data;
    }

    public void handleRequest(String key, TimestampedObject<?> value) {
        if (!this.running) {
            LOGGER.error("{}: stopped, ignore request.", name);
            return;
        }
        this.vectorClocks.tick(key, name);
        this.data.put(key, value);
        LOGGER.error("{}:request data {}={}.", name, key, value);
    }

    public void handleReplica(String key, TimestampedObject<?> requestValue,
                              String requestNode,
                              VectorClock requestVectorClock) {
        if (!this.running) {
            LOGGER.error("{}: stopped, ignore replica.", name);
            return;
        }
        VectorClock localVectorClock = this.vectorClocks.getClone(key);
        this.decider.decide(requestVectorClock, localVectorClock,
                this.vectorClocks, requestNode, key,
                new VectorClockDecider.Operator() {
                    @Override
                    public void ignore() {
                        LOGGER.error("{}->{}: request is before local, ignore it.", requestNode, name);
                    }

                    @Override
                    public void accept() {
                        LOGGER.error("{}->{}: request is after local, accept it.", requestNode, name);
                        data.put(key, requestValue);
                    }

                    @Override
                    public void conflict() {
                        LOGGER.error("{}->{}: request local is conflict, fix it.", requestNode, name);
                        data.compute(key, (k, localValue) -> {
                            if (localValue == null) {
                                return requestValue;
                            }
                            return requestValue.isAfter(localValue) ? requestValue : localValue;
                        });
                    }
                });
    }


    @Override
    public String toString() {
        return String.format("%s(%s): data:%s, vector clocks:%s", name, running ? "RUNNING" : "STOPPED", data, vectorClocks);
    }
}
package com.qwlabs.ring;

import com.google.common.collect.Lists;
import org.checkerframework.org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClusterVectorClockTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
    private List<Node> nodes;

    @BeforeEach
    void setUp() {
        nodes = Lists.newArrayList(
                new Node("A"),
                new Node("B"),
                new Node("C"),
                new Node("D")
        );
    }

    @Test
    void test() {
        LocalDateTime now = LocalDateTime.now();

        setValue("key", new TimestampedObject<>("1", now.minusMinutes(1)));
        stopAny();
        setValue("key", new TimestampedObject<>("2", now.plusMinutes(1)));
        startAny();
        setValue("key", new TimestampedObject<>("3", now.plusMinutes(4)));
        stopAny();
        setValue("key", new TimestampedObject<>("4", now.minusSeconds(3)));
        stopAny();
        setValue("key", new TimestampedObject<>("5", now.plusSeconds(2)));
        startAny();
        setValue("key", new TimestampedObject<>("6", now.minusSeconds(1)));
        startAny();
        setValue("key", new TimestampedObject<>("7", now.minusDays(1)));
    }

    private void setValue(String key, TimestampedObject<String> value) {
        Node node = randomNode(Node::isRunning).get();
        node.handleRequest(key, value);
        replicaToOther(node, key, value);
        println();
    }

    private void stopAny() {
        randomNode(Node::isRunning).ifPresent(Node::stop);
    }

    private void startAny() {
        randomNode(Node::isStopped).ifPresent(node->{
            Node runningNode = randomNode(Node::isRunning).get();
            node.start();
//          拉平差距
            runningNode.getData().forEach((key, value)->replicaToOther(runningNode, key, value));
        });
    }

    private Optional<Node> randomNode(Predicate<Node> predicate) {
        List<Node> filteredNodes = nodes.stream()
                .filter(predicate)
                .collect(Collectors.toList());
        if (filteredNodes.isEmpty()) {
            return Optional.empty();
        }
        int index = RandomUtils.nextInt(0, filteredNodes.size());
        return Optional.of(filteredNodes.get(index));
    }

    private void println() {
        LOGGER.error("------------------------------------");
        nodes.forEach(node -> LOGGER.error("{}", node));
        LOGGER.error("------------------------------------");
    }

    private void replicaToOther(Node node, String key, TimestampedObject<?> value) {
        if (!node.isRunning()) {
            return;
        }
        nodes.stream()
                .filter(toNode -> toNode != node)
                .forEach(toNode ->
                        toNode.handleReplica(key, value, node.getName(), node.getVectorClocks().getClone("key"))
                );
    }

}

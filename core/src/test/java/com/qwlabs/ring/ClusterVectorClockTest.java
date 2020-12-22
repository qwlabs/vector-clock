package com.qwlabs.ring;

import com.qwlabs.clock.NodeIds;
import com.qwlabs.clock.VectorClock;
import com.qwlabs.clock.VectorClockDecider;
import com.qwlabs.clock.VectorClocks;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.stream.Stream;

public class ClusterVectorClockTest {

    private Node nodeA = new Node("A");
    private Node nodeB = new Node("B");
    private Node nodeC = new Node("C");

    @Test
    void test() {
        LocalDateTime now = LocalDateTime.now();
        nodeA.handleRequest("key", new Value("1", now));
        replicaToOther(nodeA, nodeB, nodeC);


        nodeB.handleRequest("key", new Value("2", now));
        replicaToOther(nodeB, nodeC);
        println("A故障了");


        nodeC.handleRequest("key", new Value("3", now));
        println("B故障了");


        replicaToOther(nodeC, nodeB);
        println("B修复了");


        nodeC.handleRequest("key", new Value("4", now));
        replicaToOther(nodeC, nodeB);
        println("A依旧故障");


        replicaToOther(nodeB, nodeA);
        println("A上线了");
    }

    private void println(String message) {
        System.err.println("------------" + message  + "------------");
        System.out.println(nodeA);
        System.out.println(nodeB);
        System.out.println(nodeC);
        System.err.println("------------------------------------");
    }

    private void replicaToOther(Node node, Node... others) {
        Stream.of(others)
                .filter(toNode->toNode!=node)
                .forEach(toNode->
                    toNode.handleReplica("key", node.value, node.name, node.vectorClocks.getClone("key"))
                );
    }

    private class Value {
        private final String value;
        private final LocalDateTime timestamp;

        public Value(String value, LocalDateTime timestamp) {
            this.value = value;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return "{value:" + value + ", timestamp=" + timestamp + "}";
        }
    }

    private class Node {
        private final String name;
        private final NodeIds nodeIds;
        private final VectorClocks<String> vectorClocks;
        private Value value;

        public Node(String name) {
            this.name = name;
            nodeIds = new NodeIds();
            vectorClocks = new VectorClocks<>(nodeIds);
        }

        public void handleRequest(String key, Value value) {
            vectorClocks.tick(key, name);
            this.value = value;
        }

        public void handleReplica(String key, Value requestValue,
                                  String requestNode,
                                  VectorClock requestVectorClock) {
            VectorClock localVectorClock = vectorClocks.getClone(key);
            new VectorClockDecider().decide(requestVectorClock, localVectorClock,
                    vectorClocks, requestNode, key,
                    new VectorClockDecider.Operator() {
                        @Override
                        public void ignore() {
                            System.out.println(requestNode + "->" + name + ": request is before local, ignore it.");
                        }

                        @Override
                        public void accept() {
                            System.out.println(requestNode + "->" + name + ": request is after local, accept it.");
                            value = requestValue;
                        }

                        @Override
                        public void conflict() {
                            System.out.println(requestNode + "->" + name + ": request local is conflict, fix it.");
                            if (requestValue.timestamp.isAfter(value.timestamp)) {
                                Node.this.value = requestValue;
                            }
                        }
                    });
        }


        @Override
        public String toString() {
            return name + ": value=" + value + ", vector clocks:" + vectorClocks;
        }
    }
}

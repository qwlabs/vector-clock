package com.qwlabs.ring;

import com.qwlabs.clock.NodeIds;
import com.qwlabs.clock.VectorClocks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class VectorClocksTest {
    private VectorClocks<String> vectorClocks;

    @BeforeEach
    void setUp() {
        NodeIds nodeIds = new NodeIds();
        vectorClocks = new VectorClocks<>(nodeIds);
    }

    @Test
    void name() {
        vectorClocks.put("1").tick("node1");
        System.out.println(vectorClocks);
    }
}

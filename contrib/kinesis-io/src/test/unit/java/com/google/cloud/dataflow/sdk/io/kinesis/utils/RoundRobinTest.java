package com.google.cloud.dataflow.sdk.io.kinesis.utils;

import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Lists.newArrayList;

import static org.fest.assertions.Assertions.assertThat;
import org.junit.Test;
import java.util.Collections;
import java.util.List;

/**
 * Created by ppastuszka on 12.12.15.
 */
public class RoundRobinTest {
    @Test(expected = IllegalArgumentException.class)
    public void doesNotAllowCreationWithEmptyCollection() {
        new RoundRobin(Collections.emptyList());
    }

    @Test
    public void goesThroughElementsInCycle() {
        List<String> input = newArrayList("a", "b", "c");

        RoundRobin<String> roundRobin = new RoundRobin(newArrayList(input));

        input.addAll(input);  // duplicate the input
        for (String element : input) {
            assertThat(roundRobin.getCurrent()).isEqualTo(element);
            assertThat(roundRobin.getCurrent()).isEqualTo(element);
            roundRobin.moveForward();
        }
    }

    @Test
    public void usualIteratorGoesThroughElementsOnce() {
        List<String> input = newArrayList("a", "b", "c");

        RoundRobin<String> roundRobin = new RoundRobin(input);
        assertThat(roundRobin).hasSize(3).containsOnly(input.toArray());
    }
}

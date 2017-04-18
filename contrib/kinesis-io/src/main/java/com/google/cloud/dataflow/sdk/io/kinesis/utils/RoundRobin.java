package com.google.cloud.dataflow.sdk.io.kinesis.utils;

import static com.google.api.client.repackaged.com.google.common.base.Preconditions.checkArgument;
import static com.google.cloud.dataflow.sdk.repackaged.com.google.common.collect.Queues
        .newArrayDeque;

import java.util.Deque;
import java.util.Iterator;

/***
 * Very simple implementation of round robin algorithm.
 */
public class RoundRobin<T> implements Iterable<T> {
    private final Deque<T> deque;

    public RoundRobin(Iterable<T> collection) {
        this.deque = newArrayDeque(collection);
        checkArgument(!deque.isEmpty());
    }

    public T getCurrent() {
        return deque.getFirst();
    }

    public void moveForward() {
        deque.addLast(deque.removeFirst());
    }

    public int size() {
        return deque.size();
    }

    @Override
    public Iterator<T> iterator() {
        return deque.iterator();
    }
}

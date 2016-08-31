package com.google.cloud.dataflow.sdk.runners.inprocess;

import com.google.common.base.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * A pool of resources associated with specific keys. Implementations enforce specific use patterns,
 * such as limiting the the number of outstanding elements available per key.
 */
interface KeyedResourcePool<K, V> {
  /**
   * Tries to acquire a value for the provided key, loading it via the provided loader if necessary.
   *
   * <p>If the returned {@link Optional} contains a value, the caller obtains ownership of that
   * value. The value should be released back to this {@link KeyedResourcePool} after the
   * caller no longer has use of it using {@link #release(Object, Object)}.
   *
   * <p>The provided {@link Callable} <b>must not</b> return null; it may either return a non-null
   * value or throw an exception.
   */
  Optional<V> tryAcquire(K key, Callable<V> loader) throws ExecutionException;

  /**
   * Release the provided value, relinquishing ownership of it. Future calls to
   * {@link #tryAcquire(Object, Callable)} may return the released value.
   */
  void release(K key, V value);
}


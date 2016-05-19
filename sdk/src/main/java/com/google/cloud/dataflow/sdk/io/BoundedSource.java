/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.io;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.io.range.OffsetRangeTracker;
import com.google.cloud.dataflow.sdk.io.range.RangeTracker;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;

import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

/**
 * A {@link Source} that reads a finite amount of input and, because of that, supports
 * some additional operations.
 *
 * <p>The operations are:
 * <ul>
 * <li>Splitting into bundles of given size: {@link #splitIntoBundles};
 * <li>Size estimation: {@link #getEstimatedSizeBytes};
 * <li>Telling whether or not this source produces key/value pairs in sorted order:
 * {@link #producesSortedKeys};
 * <li>The reader ({@link BoundedReader}) supports progress estimation
 * ({@link BoundedReader#getFractionConsumed}) and dynamic splitting
 * ({@link BoundedReader#splitAtFraction}).
 * </ul>
 *
 * <p>To use this class for supporting your custom input type, derive your class
 * class from it, and override the abstract methods. For an example, see {@link DatastoreIO}.
 *
 * @param <T> Type of records read by the source.
 */
public abstract class BoundedSource<T> extends Source<T> {
  /**
   * Splits the source into bundles of approximately {@code desiredBundleSizeBytes}.
   */
  public abstract List<? extends BoundedSource<T>> splitIntoBundles(
      long desiredBundleSizeBytes, PipelineOptions options) throws Exception;

  /**
   * An estimate of the total size (in bytes) of the data that would be read from this source.
   * This estimate is in terms of external storage size, before any decompression or other
   * processing done by the reader.
   */
  public abstract long getEstimatedSizeBytes(PipelineOptions options) throws Exception;

  /**
   * Whether this source is known to produce key/value pairs sorted by lexicographic order on
   * the bytes of the encoded key.
   */
  public abstract boolean producesSortedKeys(PipelineOptions options) throws Exception;

  /**
   * Returns a new {@link BoundedReader} that reads from this source.
   */
  public abstract BoundedReader<T> createReader(PipelineOptions options) throws IOException;

  /**
   * A {@code Reader} that reads a bounded amount of input and supports some additional
   * operations, such as progress estimation and dynamic work rebalancing.
   *
   * <h3>Boundedness</h3>
   * <p>Once {@link #start} or {@link #advance} has returned false, neither will be called
   * again on this object.
   *
   * <h3>Thread safety</h3>
   * All methods will be run from the same thread except {@link #splitAtFraction},
   * {@link #getFractionConsumed}, {@link #getCurrentSource}, {@link #getParallelismConsumed()},
   * and {@link #getParallelismRemaining()}, all of which can be called concurrently
   * from a different thread. There will not be multiple concurrent calls to
   * {@link #splitAtFraction}.
   *
   * <p>It must be safe to call {@link #splitAtFraction}, {@link #getFractionConsumed},
   * {@link #getCurrentSource}, {@link #getParallelismConsumed()}, and
   * {@link #getParallelismRemaining()} concurrently with other methods.
   *
   * <p>Additionally, a successful {@link #splitAtFraction} call must, by definition, cause
   * {@link #getCurrentSource} to start returning a different value.
   * Callers of {@link #getCurrentSource} need to be aware of the possibility that the returned
   * value can change at any time, and must only access the properties of the source returned by
   * {@link #getCurrentSource} which do not change between {@link #splitAtFraction} calls.
   *
   * <h3>Implementing {@link #splitAtFraction}</h3>
   * In the course of dynamic work rebalancing, the method {@link #splitAtFraction}
   * may be called concurrently with {@link #advance} or {@link #start}. It is critical that
   * their interaction is implemented in a thread-safe way, otherwise data loss is possible.
   *
   * <p>Sources which support dynamic work rebalancing should use
   * {@link com.google.cloud.dataflow.sdk.io.range.RangeTracker} to manage the (source-specific)
   * range of positions that is being split. If your source supports dynamic work rebalancing,
   * please use that class to implement it if possible; if not possible, please contact the team
   * at <i>dataflow-feedback@google.com</i>.
   */
  @Experimental(Experimental.Kind.SOURCE_SINK)
  public abstract static class BoundedReader<T> extends Source.Reader<T> {
    /**
     * Returns a value in [0, 1] representing approximately what fraction of the
     * {@link #getCurrentSource current source} this reader has read so far, or {@code null} if such
     * an estimate is not available.
     *
     * <p>It is recommended that this method should satisfy the following properties:
     * <ul>
     *   <li>Should return 0 before the {@link #start} call.
     *   <li>Should return 1 after a {@link #start} or {@link #advance} call that returns false.
     *   <li>The returned values should be non-decreasing (though they don't have to be unique).
     * </ul>
     *
     * <p>By default, returns null to indicate that this cannot be estimated.
     *
     * <h5>Thread safety</h5>
     * If {@link #splitAtFraction} is implemented, this method can be called concurrently to other
     * methods (including itself), and it is therefore critical for it to be implemented
     * in a thread-safe way.
     */
    @Nullable
    public Double getFractionConsumed() {
      return null;
    }

    /**
     * A constant to use as the return value for {@link #getParallelismConsumed()} or
     * {@link #getParallelismRemaining()} when the exact value is unknown.
     */
    public static final long PARALLELISM_UNKNOWN = -1;

    /**
     * Returns the total amount of parallelism in the consumed (returned and processed) range of
     * this reader's current {@link BoundedSource} (as would be returned by
     * {@link #getCurrentSource}). This corresponds to all split point records (see
     * {@link RangeTracker}) returned by this reader, <em>excluding</em> the current record.
     *
     * <p>Consider the following two examples. An input that can be read in parallel down to the
     * individual records, such as {@link CountingSource#upTo}, is called "perfectly splittable".
     * Conversely, an example of a non-perfectly splittable input is a block-compressed file format
     * such as {@link AvroIO}, in which a block of records has to be read as a whole, but different
     * blocks can be read in parallel.
     *
     * <ul>
     * <li>Any {@link BoundedReader reader} that is unstarted (aka, has never had a call to
     * {@link #start}) has a consumed parallelism of 0.
     * <li>Any {@link BoundedReader reader} that has only returned its first element (aka,
     * has never had a call to {@link #advance}) has a consumed parallelism of 0: the first element
     * is the current element and is still being processed.
     * <li>When processing record #30 (starting at 1) out of 50 in a perfectly splittable 50-record
     * input, this value should be 29.
     * <li>In a block-compressed value consisting of 5 blocks, the value should stay at 0 until the
     * first record of the second block is returned; stay at 1 until the first record of the third
     * block is returned, etc. Once the end-of-file is reached then the fifth block has been
     * consumed and the value should stay at 5.
     * <li>For an empty reader (in which the call to {@link #start} returned false), the
     * consumed parallelism is 0.
     * <li>For a non-empty, finished reader (in which the call to {@link #start} returned true and
     * a call to {@link #advance} has returned false), the value returned must be at least 1
     * and should equal the total parallelism in the source. For the two examples above, the
     * 50-record file should have a consumed parallelism of 50 and the 5-block file should have
     * a consumed parallelism of 5.
     * </ul>
     *
     * <p>A reader that is implemented using a {@link RangeTracker} is encouraged to use the
     * range tracker's ability to count split points to implement this method. See
     * {@link OffsetBasedSource.OffsetBasedReader} and {@link OffsetRangeTracker} for an example.
     *
     * <p>Defaults to {@link #PARALLELISM_UNKNOWN}. Any value less than 0 will be interpreted
     * as unknown.
     *
     * <h3>Thread safety</h3>
     * See the javadoc on {@link BoundedReader} for information about thread safety.
     *
     * @see #getParallelismRemaining()
     */
    public long getParallelismConsumed() {
      return PARALLELISM_UNKNOWN;
    }

    /**
     * Returns the total amount of parallelism in the unprocessed part of this reader's current
     * {@link BoundedSource} (as would be returned by {@link #getCurrentSource}). This corresponds
     * to all unprocessed split point records (see {@link RangeTracker}), including the current
     * record, in the remainder part of the source.
     *
     * <p>This function should be implemented only <strong>in addition to
     * {@link #getParallelismConsumed()}</strong> and only if <em>an exact value can be
     * returned</em>.
     *
     * <p>Consider the following two examples. An input that can be read in parallel down to the
     * individual records, such as {@link CountingSource#upTo}, is called "perfectly splittable".
     * Conversely, an example of a non-perfectly splittable input is a block-compressed file format
     * such as {@link AvroIO}, in which a block of records has to be read as a whole, but different
     * blocks can be read in parallel.
     *
     * <ul>
     * <li>Any {@link BoundedReader reader} for which the last call to {@link #start} or
     * {@link #advance} has returned true should return a value of at least 1 to indicate the source
     * that it is currently reading.
     * <li>A finished reader for which {@link #start} or {@link #advance} has returned false
     * should return a value of 0.
     * <li>For any source that cannot be split in general, the reader should return 1.
     * <li>When processing record #30 (starting at 1) out of 50 in a perfectly splittable 50-record
     * input, this value should be 21 (20 remaining + 1 current).
     * <li>If we are reading through block 3 in a block-compressed file consisting of 5 blocks,
     * this value should be 3 (since blocks 4 and 5 can be processed in parallel by new readers
     * produced via dynamic work rebalancing, while the current reader remains processing block 3).
     * <li>If we are reading through the last block in a block-compressed file, or reading or
     * processing the last record in a perfectly splittable input, this value should be 1: apart
     * from the current task, no additional remainder can be split off.
     * </ul>
     *
     * <p>Defaults to {@link #PARALLELISM_UNKNOWN}. Any value less than 0 will be interpreted as
     * unknown.
     *
     * <h3>Thread safety</h3>
     * See the javadoc on {@link BoundedReader} for information about thread safety.
     *
     * @see #getParallelismConsumed()
     */
    public long getParallelismRemaining() {
      return PARALLELISM_UNKNOWN;
    }

    /**
     * Returns a {@code Source} describing the same input that this {@code Reader} currently reads
     * (including items already read).
     *
     * <h3>Usage</h3>
     * <p>Reader subclasses can use this method for convenience to access unchanging properties of
     * the source being read. Alternatively, they can cache these properties in the constructor.
     * <p>The framework will call this method in the course of dynamic work rebalancing, e.g. after
     * a successful {@link BoundedSource.BoundedReader#splitAtFraction} call.
     *
     * <h3>Mutability and thread safety</h3>
     * Remember that {@link Source} objects must always be immutable. However, the return value of
     * this function may be affected by dynamic work rebalancing, happening asynchronously via
     * {@link BoundedSource.BoundedReader#splitAtFraction}, meaning it can return a different
     * {@link Source} object. However, the returned object itself will still itself be immutable.
     * Callers must take care not to rely on properties of the returned source that may be
     * asynchronously changed as a result of this process (e.g. do not cache an end offset when
     * reading a file).
     *
     * <h3>Implementation</h3>
     * For convenience, subclasses should usually return the most concrete subclass of
     * {@link Source} possible.
     * In practice, the implementation of this method should nearly always be one of the following:
     * <ul>
     *   <li>Source that inherits from a base class that already implements
     *   {@link #getCurrentSource}: delegate to base class. In this case, it is almost always
     *   an error for the subclass to maintain its own copy of the source.
     * <pre>{@code
     *   public FooReader(FooSource<T> source) {
     *     super(source);
     *   }
     *
     *   public FooSource<T> getCurrentSource() {
     *     return (FooSource<T>)super.getCurrentSource();
     *   }
     * }</pre>
     *   <li>Source that does not support dynamic work rebalancing: return a private final variable.
     * <pre>{@code
     *   private final FooSource<T> source;
     *
     *   public FooReader(FooSource<T> source) {
     *     this.source = source;
     *   }
     *
     *   public FooSource<T> getCurrentSource() {
     *     return source;
     *   }
     * }</pre>
     *   <li>{@link BoundedSource.BoundedReader} that explicitly supports dynamic work rebalancing:
     *   maintain a variable pointing to an immutable source object, and protect it with
     *   synchronization.
     * <pre>{@code
     *   private FooSource<T> source;
     *
     *   public FooReader(FooSource<T> source) {
     *     this.source = source;
     *   }
     *
     *   public synchronized FooSource<T> getCurrentSource() {
     *     return source;
     *   }
     *
     *   public synchronized FooSource<T> splitAtFraction(double fraction) {
     *     ...
     *     FooSource<T> primary = ...;
     *     FooSource<T> residual = ...;
     *     this.source = primary;
     *     return residual;
     *   }
     * }</pre>
     * </ul>
     */
    @Override
    public abstract BoundedSource<T> getCurrentSource();

    /**
     * Tells the reader to narrow the range of the input it's going to read and give up
     * the remainder, so that the new range would contain approximately the given
     * fraction of the amount of data in the current range.
     *
     * <p>Returns a {@code BoundedSource} representing the remainder.
     *
     * <h5>Detailed description</h5>
     * Assuming the following sequence of calls:
     * <pre>{@code
     *   BoundedSource<T> initial = reader.getCurrentSource();
     *   BoundedSource<T> residual = reader.splitAtFraction(fraction);
     *   BoundedSource<T> primary = reader.getCurrentSource();
     * }</pre>
     * <ul>
     *  <li> The "primary" and "residual" sources, when read, should together cover the same
     *  set of records as "initial".
     *  <li> The current reader should continue to be in a valid state, and continuing to read
     *  from it should, together with the records it already read, yield the same records
     *  as would have been read by "primary".
     *  <li> The amount of data read by "primary" should ideally represent approximately
     *  the given fraction of the amount of data read by "initial".
     * </ul>
     * For example, a reader that reads a range of offsets <i>[A, B)</i> in a file might implement
     * this method by truncating the current range to <i>[A, A + fraction*(B-A))</i> and returning
     * a Source representing the range <i>[A + fraction*(B-A), B)</i>.
     *
     * <p>This method should return {@code null} if the split cannot be performed for this fraction
     * while satisfying the semantics above. E.g., a reader that reads a range of offsets
     * in a file should return {@code null} if it is already past the position in its range
     * corresponding to the given fraction. In this case, the method MUST have no effect
     * (the reader must behave as if the method hadn't been called at all).
     *
     * <h5>Statefulness</h5>
     * Since this method (if successful) affects the reader's source, in subsequent invocations
     * "fraction" should be interpreted relative to the new current source.
     *
     * <h5>Thread safety and blocking</h5>
     * This method will be called concurrently to other methods (however there will not be multiple
     * concurrent invocations of this method itself), and it is critical for it to be implemented
     * in a thread-safe way (otherwise data loss is possible).
     *
     * <p>It is also very important that this method always completes quickly. In particular,
     * it should not perform or wait on any blocking operations such as I/O, RPCs etc. Violating
     * this requirement may stall completion of the work item or even cause it to fail.
     *
     * <p>It is incorrect to make both this method and {@link #start}/{@link #advance}
     * {@code synchronized}, because those methods can perform blocking operations, and then
     * this method would have to wait for those calls to complete.
     *
     * <p>{@link com.google.cloud.dataflow.sdk.io.range.RangeTracker} makes it easy to implement
     * this method safely and correctly.
     *
     * <p>By default, returns null to indicate that splitting is not possible.
     */
    @Nullable
    public BoundedSource<T> splitAtFraction(double fraction) {
      return null;
    }

    /**
     * By default, returns the minimum possible timestamp.
     */
    @Override
    public Instant getCurrentTimestamp() throws NoSuchElementException {
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }
  }
}

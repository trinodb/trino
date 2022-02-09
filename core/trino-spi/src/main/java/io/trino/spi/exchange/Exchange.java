/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.spi.exchange;

import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface Exchange
        extends Closeable
{
    /**
     * Registers a new sink
     *
     * @param taskPartitionId uniquely identifies a dataset written to a sink
     * @return {@link ExchangeSinkHandle} associated with the <code>taskPartitionId</code>.
     * Must be passed to {@link #instantiateSink(ExchangeSinkHandle, int)} along with an attempt id to create a sink instance
     */
    ExchangeSinkHandle addSink(int taskPartitionId);

    /**
     * Called when no more sinks will be added with {@link #addSink(int)}.
     * New sink instances for an existing sink may still be added with {@link #instantiateSink(ExchangeSinkHandle, int)}.
     */
    void noMoreSinks();

    /**
     * Registers a sink instance for an attempt.
     * <p>
     * Attempts are expected to produce the same data.
     * <p>
     * The implementation must ensure the data written by unsuccessful attempts is safely discarded.
     * An attempt is considered successful when an {@link ExchangeSink} is closed with the {@link ExchangeSink#finish()} method.
     * When a node dies before {@link ExchangeSink#finish()} is called or when an {@link ExchangeSink} is closed with the
     * {@link ExchangeSink#abort()} method an attempt should be considered as unsuccessful.
     * <p>
     * When more than a single attempt is successful the implementation must pick one and discard the output of the other attempts
     *
     * @param sinkHandle - handle returned by <code>addSink</code>
     * @param taskAttemptId - attempt id
     * @return ExchangeSinkInstanceHandle to be sent to a worker that is needed to create an {@link ExchangeSink} instance using
     * {@link ExchangeManager#createSink(ExchangeSinkInstanceHandle, boolean)}
     */
    ExchangeSinkInstanceHandle instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId);

    /**
     * Called by the engine when an attempt finishes successfully
     */
    void sinkFinished(ExchangeSinkInstanceHandle handle);

    /**
     * Returns a future containing handles to be used to read data from an exchange.
     * <p>
     * Future must be resolved when the data is available to be read.
     * <p>
     * The implementation is expected to return one handle per output partition (see {@link ExchangeSink#add(int, Slice)})
     * <p>
     * Partitions can be further split if needed by calling {@link #split(ExchangeSourceHandle, long)}
     *
     * @return Future containing a list of {@link ExchangeSourceHandle} to be sent to a
     * worker that is needed to create an {@link ExchangeSource} using {@link ExchangeManager#createSource(List)}
     */
    CompletableFuture<List<ExchangeSourceHandle>> getSourceHandles();

    /**
     * Splits an {@link ExchangeSourceHandle} into a number of smaller partitions.
     * <p>
     * Exchange implementation is allowed to return {@link ExchangeSourceHandle} even before all the data
     * is written to an exchange. At the moment when the method is called it may not be possible to
     * complete the split operation. This methods returns a {@link ExchangeSourceSplitter} object
     * that allows an iterative splitting while the data is still being written to an exchange.
     *
     * @param handle returned by the {@link #getSourceHandles()}
     * @param targetSizeInBytes desired maximum size of a single partition produced by {@link ExchangeSourceSplitter}
     * @return {@link ExchangeSourceSplitter} to be used for iterative splitting of a given partition
     */
    ExchangeSourceSplitter split(ExchangeSourceHandle handle, long targetSizeInBytes);

    /**
     * Returns statistics (such as size in bytes) for a partition represented by a {@link ExchangeSourceHandle}
     *
     * @param handle returned by the {@link #getSourceHandles()} or {@link ExchangeSourceSplitter#getNext()}
     * @return object containing statistics for a given {@link ExchangeSourceHandle}
     */
    ExchangeSourceStatistics getExchangeSourceStatistics(ExchangeSourceHandle handle);

    @Override
    void close();
}

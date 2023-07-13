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

import io.trino.spi.Experimental;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
@Experimental(eta = "2023-01-01")
public interface Exchange
        extends Closeable
{
    /**
     * Get id of this exchange
     */
    ExchangeId getId();

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
     * {@link ExchangeManager#createSink(ExchangeSinkInstanceHandle)}
     */
    CompletableFuture<ExchangeSinkInstanceHandle> instantiateSink(ExchangeSinkHandle sinkHandle, int taskAttemptId);

    /**
     * Update {@link ExchangeSinkInstanceHandle}. Update is requested by {@link ExchangeSink}.
     * The updated {@link ExchangeSinkInstanceHandle} is expected to be set by {@link ExchangeSink#updateHandle(ExchangeSinkInstanceHandle)}.
     *
     * @param sinkHandle - handle returned by <code>addSink</code>
     * @param taskAttemptId - attempt id
     * @return updated handle
     */
    CompletableFuture<ExchangeSinkInstanceHandle> updateSinkInstanceHandle(ExchangeSinkHandle sinkHandle, int taskAttemptId);

    /**
     * Called by the engine when an attempt finishes successfully.
     * <p>
     * This method is expected to be lightweight. An implementation shouldn't perform any long running blocking operations within this method.
     */
    void sinkFinished(ExchangeSinkHandle sinkHandle, int taskAttemptId);

    /**
     * Called by the engine when all required sinks finished successfully.
     * While some source tasks may still be running and writing to their sinks the data written to these sinks could be safely ignored after this method is invoked.
     */
    void allRequiredSinksFinished();

    /**
     * Returns an {@link ExchangeSourceHandleSource} instance to be used to enumerate {@link ExchangeSourceHandle}s.
     *
     * @return Future containing a list of {@link ExchangeSourceHandle} to be sent to a
     * worker that is needed to create an {@link ExchangeSource} using {@link ExchangeManager#createSource()}
     */
    ExchangeSourceHandleSource getSourceHandles();

    @Override
    void close();
}

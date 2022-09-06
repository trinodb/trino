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
import io.trino.spi.Experimental;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.CompletableFuture;

@ThreadSafe
@Experimental(eta = "2023-01-01")
public interface ExchangeSink
{
    CompletableFuture<Void> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Returns {@code true} when {@link ExchangeSinkInstanceHandle} needs to be updated
     * through {@link #updateHandle(ExchangeSinkInstanceHandle)} to make further progress
     */
    boolean isHandleUpdateRequired();

    /**
     * Update {@link ExchangeSinkInstanceHandle}. Done by the engine upon request initiated by the {@link ExchangeSink}
     *
     * @param handle updated handle
     */
    void updateHandle(ExchangeSinkInstanceHandle handle);

    /**
     * Returns a future that will be completed when the exchange sink becomes
     * unblocked.  If the exchange sink is not blocked, this method should return
     * {@code NOT_BLOCKED}
     */
    CompletableFuture<Void> isBlocked();

    /**
     * Appends arbitrary {@code data} to a partition specified by {@code partitionId}.
     * With method call the {@code data} buffer ownership is passed from caller to callee.
     * This method is guaranteed not to be invoked after {@link #finish()}.
     * This method can be invoked after {@link #abort()}.
     * If this method is invoked after {@link #abort()} the invocation should be ignored.
     */
    void add(int partitionId, Slice data);

    /**
     * Get the total memory that needs to be reserved in the memory pool.
     * This memory should include any buffers, etc. that are used for writing data
     */
    long getMemoryUsage();

    /**
     * Notifies the exchange sink that no more data will be appended.
     * This method is guaranteed not to be called after {@link #abort()}.
     * This method is guaranteed not be called more than once.
     * The {@link #abort()} method will not be called if the finish operation fails.
     * The finish implementation is responsible for safely releasing resources in
     * case of a failure.
     *
     * @return future that will be resolved when the finish operation either succeeds or fails
     */
    CompletableFuture<Void> finish();

    /**
     * Notifies the exchange that the write operation has been aborted.
     * This method may be called when {@link #finish()} is still running. In this situation the implementation
     * is free to either cancel the finish operation and abort or let the finish operation succeed.
     * This method may also be called when {@link #finish()} is done. In this situation the implementation
     * is free to either ignore the call or invalidate the sink.
     * This method is guaranteed not be called more than once.
     *
     * @return future that will be resolved when the abort operation either succeeds or fails
     */
    CompletableFuture<Void> abort();
}

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

import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface ExchangeSink
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Returns a future that will be completed when the exchange sink becomes
     * unblocked.  If the exchange sink is not blocked, this method should return
     * {@code NOT_BLOCKED}
     */
    CompletableFuture<?> isBlocked();

    /**
     * Appends arbitrary {@code data} to a partition specified by {@code partitionId}.
     * The engine is free to reuse the {@code data} buffer.
     * The implementation is expected to copy the buffer as it may be invalidated and recycled.
     * If this method is invoked after {@link #finish()} or {@link #abort()} is initiated the
     * invocation should be ignored.
     */
    void add(int partitionId, Slice data);

    /**
     * Get the total memory that needs to be reserved in the general memory pool.
     * This memory should include any buffers, etc. that are used for writing data
     */
    long getMemoryUsage();

    /**
     * Notifies the exchange sink that no more data will be appended.
     * This method is guaranteed not to be called after {@link #abort()}.
     * This method is guaranteed not be called more than once.
     *
     * @return future that will be resolved when the finish operation either succeeds or fails
     */
    CompletableFuture<?> finish();

    /**
     * Notifies the exchange that the write operation has been aborted.
     * This method may be called when {@link #finish()} is still running. In this situation the implementation
     * is free to either cancel the finish operation and abort or let the finish operation succeed.
     * This method is guaranteed not be called more than once.
     *
     * @return future that will be resolved when the abort operation either succeeds or fails
     */
    CompletableFuture<?> abort();
}

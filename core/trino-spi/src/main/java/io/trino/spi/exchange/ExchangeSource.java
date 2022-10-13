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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
@Experimental(eta = "2023-01-01")
public interface ExchangeSource
        extends Closeable
{
    CompletableFuture<Void> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Add more {@link ExchangeSourceHandle}'s to be read by the {@link ExchangeSource}.
     *
     * @param handles list of {@link ExchangeSourceHandle}'s describing what exchange data to
     * read. The full list of handles is returned by {@link ExchangeSourceHandleSource}.
     * The coordinator decides what items from that list should be handled by what task and creates
     * sub-lists that are further getting sent to a worker to be read.
     * The <code>handles</code> list may contain {@link ExchangeSourceHandle}'s created by more than
     * a single {@link Exchange}.
     */
    void addSourceHandles(List<ExchangeSourceHandle> handles);

    /**
     * Notify {@link ExchangeSource} that no more {@link ExchangeSourceHandle}'s will be added with the
     * {@link #addSourceHandles(List)} method.
     */
    void noMoreSourceHandles();

    /**
     * Called by the engine to provide information about what source task output must be included
     * and what must be skipped.
     * <p>
     * This method can be called multiple times and out of order.
     * Only a newest version (see {@link ExchangeSourceOutputSelector#getVersion()}) must be taken into account.
     * Updates with an older version must be ignored.
     * <p>
     * The information provided by the {@link ExchangeSourceOutputSelector} is incremental and decisions
     * for some partitions could be missing. The implementation is free to speculate.
     * <p>
     * The final selector is guaranteed to contain a decision for each source partition (see {@link ExchangeSourceOutputSelector#isFinal()}).
     * If decision is made for a given partition in some version the decision is guaranteed not to change in newer versions.
     */
    void setOutputSelector(ExchangeSourceOutputSelector selector);

    /**
     * Returns a future that will be completed when the exchange source becomes
     * unblocked.  If the exchange source is not blocked, this method should return
     * {@code NOT_BLOCKED}
     */
    CompletableFuture<Void> isBlocked();

    /**
     * Once isFinished returns true, {@link #read()} will never return a non-null result
     */
    boolean isFinished();

    /**
     * Gets the next chunk of data. This method is allowed to return null.
     * <p>
     * The engine will keep calling this method until {@link #isFinished()} returns {@code true}
     *
     * @return data written to an exchange using {@link ExchangeSink#add(int, Slice)}.
     * The slice is always returned as a whole as written (the exchange does not split and does not merge slices).
     */
    @Nullable
    Slice read();

    /**
     * Get the total memory that needs to be reserved in the memory pool.
     * This memory should include any buffers, etc. that are used for reading data
     */
    long getMemoryUsage();

    @Override
    void close();
}

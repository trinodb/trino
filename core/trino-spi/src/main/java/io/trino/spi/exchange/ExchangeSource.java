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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public interface ExchangeSource
        extends Closeable
{
    CompletableFuture<?> NOT_BLOCKED = CompletableFuture.completedFuture(null);

    /**
     * Returns a future that will be completed when the exchange source becomes
     * unblocked.  If the exchange source is not blocked, this method should return
     * {@code NOT_BLOCKED}
     */
    CompletableFuture<?> isBlocked();

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
     * Get the total memory that needs to be reserved in the general memory pool.
     * This memory should include any buffers, etc. that are used for reading data
     */
    long getMemoryUsage();

    @Override
    void close();
}

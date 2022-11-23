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

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public interface ExchangeSourceHandleSource
        extends Closeable
{
    /**
     * Returns a next batch of {@link ExchangeSourceHandle}s.
     * <p>
     * Cannot be called when a future returned by a previous invocation is not yet finished.
     * <p>
     * {@link ExchangeSourceHandleBatch#lastBatch()} returns true when finished.
     *
     * @return a future containing a batch of {@link ExchangeSourceHandle}s.
     */
    CompletableFuture<ExchangeSourceHandleBatch> getNextBatch();

    @Override
    void close();

    record ExchangeSourceHandleBatch(List<ExchangeSourceHandle> handles, boolean lastBatch)
    {
        public ExchangeSourceHandleBatch
        {
            handles = List.copyOf(requireNonNull(handles, "handles is null"));
        }
    }
}

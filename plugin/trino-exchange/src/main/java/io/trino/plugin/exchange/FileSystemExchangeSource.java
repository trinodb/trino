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
package io.trino.plugin.exchange;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSource;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private final List<ExchangeStorageReader> readers;
    private volatile boolean closed;

    public FileSystemExchangeSource(
            FileSystemExchangeStorage exchangeStorage,
            List<ExchangeSourceFile> sourceFiles,
            int maxPageStorageSize,
            int exchangeSourceConcurrentReaders)
    {
        requireNonNull(exchangeStorage, "exchangeStorage is null");
        Queue<ExchangeSourceFile> sourceFileQueue = new ArrayBlockingQueue<>(sourceFiles.size());
        sourceFileQueue.addAll(sourceFiles);

        int numReaders = min(sourceFiles.size(), exchangeSourceConcurrentReaders);

        ImmutableList.Builder<ExchangeStorageReader> readers = ImmutableList.builder();
        for (int i = 0; i < numReaders; ++i) {
            readers.add(exchangeStorage.createExchangeStorageReader(sourceFileQueue, maxPageStorageSize));
        }
        this.readers = readers.build();
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone()) {
                return NOT_BLOCKED;
            }
        }
        return toCompletableFuture(
                nonCancellationPropagating(
                        whenAnyComplete(readers.stream()
                                .map(ExchangeStorageReader::isBlocked)
                                .collect(toImmutableList()))));
    }

    @Override
    public boolean isFinished()
    {
        if (closed) {
            return true;
        }

        for (ExchangeStorageReader reader : readers) {
            if (!reader.isFinished()) {
                return false;
            }
        }
        return true;
    }

    @Nullable
    @Override
    public Slice read()
    {
        if (closed) {
            return null;
        }

        for (ExchangeStorageReader reader : readers) {
            if (reader.isBlocked().isDone() && !reader.isFinished()) {
                try {
                    return reader.read();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        return null;
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = 0;
        for (ExchangeStorageReader reader : readers) {
            memoryUsage += reader.getRetainedSize();
        }
        return memoryUsage;
    }

    @Override
    public void close()
    {
        // Make sure we will only close once
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
        }

        readers.forEach(ExchangeStorageReader::close);
    }
}

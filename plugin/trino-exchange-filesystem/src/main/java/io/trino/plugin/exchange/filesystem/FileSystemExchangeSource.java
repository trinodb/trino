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
package io.trino.plugin.exchange.filesystem;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final int maxPageStorageSize;
    private final int exchangeSourceConcurrentReaders;

    private final Queue<ExchangeSourceFile> files = new ConcurrentLinkedQueue<>();
    @GuardedBy("this")
    private boolean noMoreFiles;
    @GuardedBy("this")
    private SettableFuture<Void> blockedOnSourceHandles = SettableFuture.create();

    private final AtomicReference<List<ExchangeStorageReader>> readers = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<CompletableFuture<Void>> blocked = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public FileSystemExchangeSource(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            int maxPageStorageSize,
            int exchangeSourceConcurrentReaders)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.maxPageStorageSize = maxPageStorageSize;
        this.exchangeSourceConcurrentReaders = exchangeSourceConcurrentReaders;
    }

    @Override
    public synchronized void addSourceHandles(List<ExchangeSourceHandle> handles)
    {
        if (closed.get()) {
            return;
        }
        files.addAll(getFiles(handles));
        closeAndCreateReadersIfNecessary();
    }

    @Override
    public synchronized void noMoreSourceHandles()
    {
        noMoreFiles = true;
        closeAndCreateReadersIfNecessary();
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        if (closed.get()) {
            return NOT_BLOCKED;
        }

        CompletableFuture<Void> blocked = this.blocked.get();
        if (blocked != null && !blocked.isDone()) {
            return blocked;
        }

        List<ExchangeStorageReader> readers = this.readers.get();
        // regular loop for efficiency
        for (int i = 0; i < readers.size(); i++) {
            ExchangeStorageReader reader = readers.get(i);
            if (reader.isBlocked().isDone()) {
                return NOT_BLOCKED;
            }
        }

        synchronized (this) {
            if (!blockedOnSourceHandles.isDone()) {
                blocked = toCompletableFuture(nonCancellationPropagating(blockedOnSourceHandles));
            }
            else if (readers.isEmpty()) {
                blocked = NOT_BLOCKED;
            }
            else {
                blocked = toCompletableFuture(
                        nonCancellationPropagating(
                                whenAnyComplete(readers.stream()
                                        .map(ExchangeStorageReader::isBlocked)
                                        .collect(toImmutableList()))));
            }
            blocked = stats.getExchangeSourceBlocked().record(blocked);
            this.blocked.set(blocked);
            return blocked;
        }
    }

    @Override
    public boolean isFinished()
    {
        return closed.get();
    }

    @Nullable
    @Override
    public Slice read()
    {
        if (closed.get()) {
            return null;
        }

        Slice data = null;
        List<ExchangeStorageReader> readers = this.readers.get();
        // regular loop for efficiency
        for (int i = 0; i < readers.size(); i++) {
            ExchangeStorageReader reader = readers.get(i);
            if (reader.isBlocked().isDone() && !reader.isFinished()) {
                try {
                    data = reader.read();
                    break;
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        closeAndCreateReadersIfNecessary();

        return data;
    }

    @Override
    public long getMemoryUsage()
    {
        long memoryUsage = 0;
        List<ExchangeStorageReader> readers = this.readers.get();
        // regular loop for efficiency
        for (int i = 0; i < readers.size(); i++) {
            memoryUsage += readers.get(i).getRetainedSize();
        }
        return memoryUsage;
    }

    @Override
    public synchronized void close()
    {
        // Make sure we will only close once
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        files.clear();
        Closer closer = Closer.create();
        for (ExchangeStorageReader reader : readers.getAndSet(ImmutableList.of())) {
            closer.register(reader);
        }
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void closeAndCreateReadersIfNecessary()
    {
        int numberOfActiveReaders = getNumberOfActiveReaders();
        if (numberOfActiveReaders == exchangeSourceConcurrentReaders) {
            return;
        }
        if (numberOfActiveReaders > 0 && files.isEmpty()) {
            return;
        }

        SettableFuture<Void> blockedOnSourceHandlesToBeUnblocked = null;
        synchronized (this) {
            if (closed.get()) {
                return;
            }

            List<ExchangeStorageReader> activeReaders = new ArrayList<>();
            for (ExchangeStorageReader reader : readers.get()) {
                if (reader.isFinished()) {
                    reader.close();
                }
                else {
                    activeReaders.add(reader);
                }
            }
            try {
                while (activeReaders.size() < exchangeSourceConcurrentReaders && !files.isEmpty()) {
                    ImmutableList.Builder<ExchangeSourceFile> readerFiles = ImmutableList.builder();
                    long readerFileSize = 0;
                    while (!files.isEmpty()) {
                        ExchangeSourceFile file = files.peek();
                        if (readerFileSize == 0 || readerFileSize + file.getFileSize() <= maxPageStorageSize + exchangeStorage.getWriteBufferSize()) {
                            readerFiles.add(file);
                            readerFileSize += file.getFileSize();
                            files.poll();
                        }
                        else {
                            break;
                        }
                    }
                    activeReaders.add(exchangeStorage.createExchangeStorageReader(readerFiles.build(), maxPageStorageSize));
                }
                if (activeReaders.isEmpty()) {
                    if (noMoreFiles) {
                        blockedOnSourceHandlesToBeUnblocked = blockedOnSourceHandles;
                        close();
                    }
                    else if (blockedOnSourceHandles.isDone()) {
                        blockedOnSourceHandles = SettableFuture.create();
                    }
                }
                else if (!blockedOnSourceHandles.isDone()) {
                    blockedOnSourceHandlesToBeUnblocked = blockedOnSourceHandles;
                }
                this.readers.set(ImmutableList.copyOf(activeReaders));
            }
            catch (Throwable t) {
                for (ExchangeStorageReader reader : activeReaders) {
                    try {
                        reader.close();
                    }
                    catch (Throwable closeFailure) {
                        if (closeFailure != t) {
                            t.addSuppressed(closeFailure);
                        }
                    }
                }
                throw t;
            }
        }
        if (blockedOnSourceHandlesToBeUnblocked != null) {
            blockedOnSourceHandlesToBeUnblocked.set(null);
        }
    }

    private int getNumberOfActiveReaders()
    {
        List<ExchangeStorageReader> readers = this.readers.get();
        int result = 0;
        // regular loop for efficiency
        for (int i = 0; i < readers.size(); i++) {
            ExchangeStorageReader reader = readers.get(i);
            if (!reader.isFinished()) {
                result++;
            }
        }
        return result;
    }

    private static List<ExchangeSourceFile> getFiles(List<ExchangeSourceHandle> handles)
    {
        return handles.stream()
                .map(FileSystemExchangeSourceHandle.class::cast)
                .map(handle -> {
                    Optional<SecretKey> secretKey = handle.getSecretKey().map(key -> new SecretKeySpec(key, 0, key.length, "AES"));
                    return new AbstractMap.SimpleEntry<>(handle, secretKey);
                })
                .flatMap(entry -> entry.getKey().getFiles().stream().map(fileStatus ->
                        new ExchangeSourceFile(
                                URI.create(fileStatus.getFilePath()),
                                entry.getValue(),
                                fileStatus.getFileSize())))
                .collect(toImmutableList());
    }
}

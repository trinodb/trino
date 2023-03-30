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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.trino.spi.exchange.ExchangeSource;
import io.trino.spi.exchange.ExchangeSourceHandle;
import io.trino.spi.exchange.ExchangeSourceOutputSelector;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.trino.spi.exchange.ExchangeSourceOutputSelector.Selection.INCLUDED;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSource
        implements ExchangeSource
{
    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final int maxPageStorageSize;
    private final int exchangeSourceConcurrentReaders;
    private final int maxFilesPerReader;

    private final Queue<ExchangeSourceFile> files = new ConcurrentLinkedQueue<>();
    @GuardedBy("this")
    private boolean noMoreFiles;
    @GuardedBy("this")
    private ExchangeSourceOutputSelector currentSelector;
    @GuardedBy("this")
    private SettableFuture<Void> blockedOnFiles = SettableFuture.create();

    private final AtomicReference<List<ExchangeStorageReader>> readers = new AtomicReference<>(ImmutableList.of());
    private final AtomicReference<ListenableFuture<Void>> blocked = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public FileSystemExchangeSource(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            int maxPageStorageSize,
            int exchangeSourceConcurrentReaders,
            int maxFilesPerReader)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.maxPageStorageSize = maxPageStorageSize;
        this.exchangeSourceConcurrentReaders = exchangeSourceConcurrentReaders;
        this.maxFilesPerReader = maxFilesPerReader;
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
    public synchronized void setOutputSelector(ExchangeSourceOutputSelector newSelector)
    {
        if (currentSelector != null) {
            if (currentSelector.getVersion() >= newSelector.getVersion()) {
                return;
            }
            currentSelector.checkValidTransition(newSelector);
        }
        currentSelector = newSelector;
        closeAndCreateReadersIfNecessary();
    }

    @Override
    public CompletableFuture<Void> isBlocked()
    {
        if (closed.get()) {
            return NOT_BLOCKED;
        }

        ListenableFuture<Void> blocked = this.blocked.get();
        if (blocked != null && !blocked.isDone()) {
            return nonCancellationPropagatingCompletableFuture(blocked);
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
            if (!blockedOnFiles.isDone()) {
                blocked = blockedOnFiles;
            }
            else if (readers.isEmpty()) {
                // not blocked
                blocked = immediateVoidFuture();
            }
            else {
                blocked = whenAnyComplete(readers.stream()
                        .map(ExchangeStorageReader::isBlocked)
                        .collect(toImmutableList()));
            }
            blocked = stats.getExchangeSourceBlocked().record(blocked);
            this.blocked.set(blocked);
        }

        return nonCancellationPropagatingCompletableFuture(blocked);
    }

    private static CompletableFuture<Void> nonCancellationPropagatingCompletableFuture(ListenableFuture<Void> future)
    {
        CompletableFuture<Void> result = new CompletableFuture<>();
        Futures.addCallback(future, new FutureCallback<>()
        {
            @Override
            public void onSuccess(Void value)
            {
                result.complete(value);
            }

            @Override
            public void onFailure(Throwable t)
            {
                result.completeExceptionally(t);
            }
        }, directExecutor());
        return result;
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

        SettableFuture<Void> blockedOnFilesToBeUnblocked = null;
        synchronized (this) {
            if (closed.get()) {
                return;
            }

            if (currentSelector == null || !currentSelector.isFinal()) {
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
                    int readerFileCount = 0;
                    long readerFileSize = 0;
                    while (!files.isEmpty()) {
                        ExchangeSourceFile file = files.peek();
                        verify(currentSelector.getSelection(file.getExchangeId(), file.getSourceTaskPartitionId(), file.getSourceTaskAttemptId()) == INCLUDED,
                                "%s.%s.%s is not marked as included by the engine",
                                file.getExchangeId(),
                                file.getSourceTaskPartitionId(),
                                file.getSourceTaskAttemptId());
                        if (readerFileCount == 0 || ((readerFileSize + file.getFileSize() <= maxPageStorageSize + exchangeStorage.getWriteBufferSize()) && readerFileCount < maxFilesPerReader)) {
                            readerFiles.add(file);
                            readerFileSize += file.getFileSize();
                            readerFileCount++;
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
                        blockedOnFilesToBeUnblocked = blockedOnFiles;
                        close();
                    }
                    else if (blockedOnFiles.isDone()) {
                        blockedOnFiles = SettableFuture.create();
                    }
                }
                else if (!blockedOnFiles.isDone()) {
                    blockedOnFilesToBeUnblocked = blockedOnFiles;
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
        if (blockedOnFilesToBeUnblocked != null) {
            blockedOnFilesToBeUnblocked.set(null);
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
                .flatMap(handle -> handle.getFiles().stream().map(sourceFile ->
                        new ExchangeSourceFile(
                                URI.create(sourceFile.getFilePath()),
                                sourceFile.getFileSize(),
                                handle.getExchangeId(),
                                sourceFile.getSourceTaskPartitionId(),
                                sourceFile.getSourceTaskAttemptId())))
                .collect(toImmutableList());
    }
}

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

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.spi.exchange.ExchangeSink;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.GuardedBy;
import javax.crypto.SecretKey;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSink
        implements ExchangeSink
{
    private static final Logger log = Logger.get(FileSystemExchangeSink.class);

    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FileSystemExchangeSink.class).instanceSize();

    private final FileSystemExchangeStorage exchangeStorage;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final int numBuffers;
    private final Optional<SecretKey> secretKey;
    private final int writeBufferSize;

    private final Map<Integer, ExchangeStorageWriter> writers = new ConcurrentHashMap<>();
    private final Map<Integer, SliceOutput> pendingBuffers = new ConcurrentHashMap<>();
    private final LinkedBlockingQueue<SliceOutput> freeBuffers = new LinkedBlockingQueue<>();
    private volatile boolean committed;
    private volatile boolean closed;
    @GuardedBy("this")
    private CompletableFuture<Void> blockedFuture = new CompletableFuture<>();

    public FileSystemExchangeSink(FileSystemExchangeStorage exchangeStorage, URI outputDirectory, int outputPartitionCount, Optional<SecretKey> secretKey)
    {
        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.numBuffers = outputPartitionCount * 2;
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.writeBufferSize = exchangeStorage.getWriteBufferSizeInBytes();

        for (int i = 0; i < numBuffers; ++i) {
            freeBuffers.add(Slices.allocate(writeBufferSize).getOutput());
        }
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (freeBuffers.isEmpty() && pendingBuffers.size() < outputPartitionCount) {
            synchronized (this) {
                if (blockedFuture.isDone()) {
                    blockedFuture = new CompletableFuture<>();
                }
                return blockedFuture;
            }
        }
        else {
            return NOT_BLOCKED;
        }
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);
        checkState(!committed, "already committed");
        if (closed) {
            return;
        }

        writers.computeIfAbsent(partitionId, this::createWriter);
        synchronized (writers.get(partitionId)) {
            writeToExchangeStorage(partitionId, Slices.wrappedIntArray(data.length()));
            writeToExchangeStorage(partitionId, data);
        }
    }

    private ExchangeStorageWriter createWriter(int partitionId)
    {
        URI outputPath = outputDirectory.resolve(partitionId + DATA_FILE_SUFFIX);
        try {
            return exchangeStorage.createExchangeStorageWriter(outputPath, secretKey);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeToExchangeStorage(int partitionId, Slice slice)
    {
        int position = 0;
        while (position < slice.length()) {
            SliceOutput pendingBuffer = pendingBuffers.computeIfAbsent(partitionId, ignored -> {
                try {
                    return freeBuffers.take();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
            int writableBytes = min(pendingBuffer.writableBytes(), slice.length() - position);
            pendingBuffer.writeBytes(slice.getBytes(position, writableBytes));
            position += writableBytes;

            flushIfNeeded(partitionId, false);
        }
    }

    private void flushIfNeeded(int partitionId, boolean finished)
    {
        SliceOutput buffer = pendingBuffers.get(partitionId);
        if (!buffer.isWritable() || finished) {
            if (!buffer.isWritable()) {
                pendingBuffers.remove(partitionId);
            }
            try {
                writers.get(partitionId).write(buffer.slice()).addListener(() -> {
                    buffer.reset();
                    freeBuffers.add(buffer);
                    synchronized (this) {
                        if (!blockedFuture.isDone()) {
                            blockedFuture.complete(null);
                        }
                    }
                }, directExecutor());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + (long) numBuffers * writeBufferSize;
    }

    @Override
    public void finish()
    {
        if (closed) {
            return;
        }
        try {
            for (Integer partitionId : writers.keySet()) {
                flushIfNeeded(partitionId, true);
                try {
                    pendingBuffers.get(partitionId).close();
                    writers.get(partitionId).close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            for (SliceOutput freeBuffer : freeBuffers) {
                try {
                    freeBuffer.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            try {
                exchangeStorage.createEmptyFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        catch (Throwable t) {
            abort();
            throw t;
        }
        pendingBuffers.clear();
        freeBuffers.clear();
        committed = true;
        closed = true;
    }

    @Override
    public void abort()
    {
        if (closed) {
            return;
        }
        closed = true;
        for (Integer partitionId : writers.keySet()) {
            try {
                pendingBuffers.get(partitionId).close();
                writers.get(partitionId).close();
            }
            catch (IOException e) {
                log.warn(e, "Error closing pending buffer and writer for exchanges");
            }
        }
        for (SliceOutput freeBuffer : freeBuffers) {
            try {
                freeBuffer.close();
            }
            catch (IOException e) {
                log.warn(e, "Error closing free buffer for exchanges");
            }
        }
        pendingBuffers.clear();
        freeBuffers.clear();
        try {
            exchangeStorage.deleteRecursively(outputDirectory);
        }
        catch (IOException e) {
            log.warn(e, "Error cleaning output directory");
        }
    }
}

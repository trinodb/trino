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
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.trino.plugin.exchange.filesystem.MetricsBuilder.CounterMetricBuilder;
import io.trino.plugin.exchange.filesystem.MetricsBuilder.DistributionMetricBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.exchange.ExchangeSink;
import io.trino.spi.exchange.ExchangeSinkInstanceHandle;
import io.trino.spi.metrics.Metrics;

import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.addExceptionCallback;
import static io.airlift.concurrent.MoreFutures.addSuccessCallback;
import static io.airlift.concurrent.MoreFutures.asVoid;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.succinctBytes;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

@ThreadSafe
public class FileSystemExchangeSink
        implements ExchangeSink
{
    public static final String COMMITTED_MARKER_FILE_NAME = "committed";
    public static final String DATA_FILE_SUFFIX = ".data";

    private static final int INSTANCE_SIZE = instanceSize(FileSystemExchangeSink.class);

    private final FileSystemExchangeStorage exchangeStorage;
    private final FileSystemExchangeStats stats;
    private final URI outputDirectory;
    private final int outputPartitionCount;
    private final boolean preserveOrderWithinPartition;
    private final int maxPageStorageSizeInBytes;
    private final long maxFileSizeInBytes;
    private final BufferPool bufferPool;

    private final Map<Integer, BufferedStorageWriter> writersMap = new ConcurrentHashMap<>();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    private final MetricsBuilder metricsBuilder = new MetricsBuilder();
    private final CounterMetricBuilder totalFilesMetric = metricsBuilder.getCounterMetric("FileSystemExchangeSource.filesTotal");
    private final DistributionMetricBuilder fileSizeMetric = metricsBuilder.getDistributionMetric("FileSystemExchangeSource.fileSize");

    public FileSystemExchangeSink(
            FileSystemExchangeStorage exchangeStorage,
            FileSystemExchangeStats stats,
            URI outputDirectory,
            int outputPartitionCount,
            boolean preserveOrderWithinPartition,
            int maxPageStorageSizeInBytes,
            int exchangeSinkBufferPoolMinSize,
            int exchangeSinkBuffersPerPartition,
            long maxFileSizeInBytes)
    {
        checkArgument(maxPageStorageSizeInBytes <= maxFileSizeInBytes,
                format("maxPageStorageSizeInBytes %s exceeded maxFileSizeInBytes %s", succinctBytes(maxPageStorageSizeInBytes), succinctBytes(maxFileSizeInBytes)));

        this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
        this.outputPartitionCount = outputPartitionCount;
        this.preserveOrderWithinPartition = preserveOrderWithinPartition;
        this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
        this.maxFileSizeInBytes = maxFileSizeInBytes;
        // buffer pooling to overlap computation and I/O
        this.bufferPool = new BufferPool(stats, max(outputPartitionCount * exchangeSinkBuffersPerPartition, exchangeSinkBufferPoolMinSize), exchangeStorage.getWriteBufferSize());
    }

    @Override
    public boolean isHandleUpdateRequired()
    {
        return false;
    }

    @Override
    public void updateHandle(ExchangeSinkInstanceHandle handle)
    {
        // this implementation never requests an update
        throw new UnsupportedOperationException();
    }

    // The future returned by {@link #isBlocked()} should only be considered as a best-effort hint.
    @Override
    public CompletableFuture<Void> isBlocked()
    {
        return bufferPool.isBlocked();
    }

    @Override
    public void add(int partitionId, Slice data)
    {
        throwIfFailed();

        checkArgument(partitionId < outputPartitionCount, "partition id is expected to be less than %s: %s", outputPartitionCount, partitionId);

        // Ensure no new writers can be created after `closed` is set to true
        BufferedStorageWriter writer;
        synchronized (this) {
            if (closed) {
                return;
            }
            writer = writersMap.computeIfAbsent(partitionId, this::createWriter);
        }
        writer.write(data);
    }

    private BufferedStorageWriter createWriter(int partitionId)
    {
        return new BufferedStorageWriter(
                exchangeStorage,
                stats,
                outputDirectory,
                preserveOrderWithinPartition,
                partitionId,
                bufferPool,
                failure,
                maxPageStorageSizeInBytes,
                maxFileSizeInBytes,
                totalFilesMetric,
                fileSizeMetric);
    }

    @Override
    public long getMemoryUsage()
    {
        return INSTANCE_SIZE
                + bufferPool.getRetainedSize()
                + estimatedSizeOf(writersMap, SizeOf::sizeOf, BufferedStorageWriter::getRetainedSize);
    }

    @Override
    public synchronized CompletableFuture<Void> finish()
    {
        if (closed) {
            return failedFuture(new IllegalStateException("Exchange sink has already closed"));
        }

        ListenableFuture<Void> finishFuture = asVoid(Futures.allAsList(
                writersMap.values().stream().map(BufferedStorageWriter::finish).collect(toImmutableList())));
        addSuccessCallback(finishFuture, this::destroy);
        finishFuture = Futures.transformAsync(
                finishFuture,
                _ -> exchangeStorage.createEmptyFile(outputDirectory.resolve(COMMITTED_MARKER_FILE_NAME)),
                directExecutor());
        Futures.addCallback(finishFuture, new FutureCallback<>()
        {
            @Override
            public void onSuccess(Void result)
            {
                closed = true;
            }

            @Override
            public void onFailure(Throwable ignored)
            {
                abort();
            }
        }, directExecutor());

        return stats.getExchangeSinkFinish().record(toCompletableFuture(finishFuture));
    }

    @Override
    public synchronized CompletableFuture<Void> abort()
    {
        if (closed) {
            return completedFuture(null);
        }
        closed = true;

        ListenableFuture<Void> abortFuture = asVoid(Futures.allAsList(
                writersMap.values().stream().map(BufferedStorageWriter::abort).collect(toImmutableList())));
        addSuccessCallback(abortFuture, this::destroy);

        return stats.getExchangeSinkAbort().record(toCompletableFuture(Futures.transformAsync(
                abortFuture,
                _ -> exchangeStorage.deleteRecursively(ImmutableList.of(outputDirectory)),
                directExecutor())));
    }

    @Override
    public Optional<Metrics> getMetrics()
    {
        return Optional.of(metricsBuilder.buildMetrics());
    }

    private void throwIfFailed()
    {
        Throwable throwable = failure.get();
        if (throwable != null) {
            throwIfUnchecked(throwable);
            throw new RuntimeException(throwable);
        }
    }

    private void destroy()
    {
        writersMap.clear();
        bufferPool.close();
    }

    @ThreadSafe
    private static class BufferedStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(BufferedStorageWriter.class);

        private final FileSystemExchangeStorage exchangeStorage;
        private final FileSystemExchangeStats stats;
        private final URI outputDirectory;
        private final boolean preserveOrderWithinPartition;
        private final int partitionId;
        private final BufferPool bufferPool;
        private final AtomicReference<Throwable> failure;
        private final int maxPageStorageSizeInBytes;
        private final long maxFileSizeInBytes;
        private final CounterMetricBuilder totalFilesMetric;
        private final DistributionMetricBuilder fileSizeMetric;

        @GuardedBy("this")
        private ExchangeStorageWriter currentWriter;
        @GuardedBy("this")
        private long currentFileSize;
        @GuardedBy("this")
        private SliceOutput currentBuffer;
        @GuardedBy("this")
        private final List<ExchangeStorageWriter> writers = new ArrayList<>();
        @GuardedBy("this")
        private boolean closed;

        public BufferedStorageWriter(
                FileSystemExchangeStorage exchangeStorage,
                FileSystemExchangeStats stats,
                URI outputDirectory,
                boolean preserveOrderWithinPartition,
                int partitionId,
                BufferPool bufferPool,
                AtomicReference<Throwable> failure,
                int maxPageStorageSizeInBytes,
                long maxFileSizeInBytes,
                CounterMetricBuilder totalFilesMetric,
                DistributionMetricBuilder fileSizeMetric)
        {
            this.exchangeStorage = requireNonNull(exchangeStorage, "exchangeStorage is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.outputDirectory = requireNonNull(outputDirectory, "outputDirectory is null");
            this.preserveOrderWithinPartition = preserveOrderWithinPartition;
            this.partitionId = partitionId;
            this.bufferPool = requireNonNull(bufferPool, "bufferPool is null");
            this.failure = requireNonNull(failure, "failure is null");
            this.maxPageStorageSizeInBytes = maxPageStorageSizeInBytes;
            this.maxFileSizeInBytes = maxFileSizeInBytes;
            this.totalFilesMetric = requireNonNull(totalFilesMetric, "totalFilesMetric is null");
            this.fileSizeMetric = requireNonNull(fileSizeMetric, "fileSizeMetric is null");

            setupWriterForNextPart();
        }

        public synchronized void write(Slice data)
        {
            if (closed) {
                return;
            }

            int requiredPageStorageSize = Integer.BYTES + data.length();
            if (requiredPageStorageSize > maxPageStorageSizeInBytes) {
                throw new TrinoException(NOT_SUPPORTED, format("Max row size of %s exceeded: %s", succinctBytes(maxPageStorageSizeInBytes), succinctBytes(requiredPageStorageSize)));
            }

            if (currentFileSize + requiredPageStorageSize > maxFileSizeInBytes && !preserveOrderWithinPartition) {
                stats.getFileSizeInBytes().add(currentFileSize);
                flushIfNeeded(true);
                setupWriterForNextPart();
                currentFileSize = 0;
                currentBuffer = null;
                fileSizeMetric.add(currentFileSize);
                totalFilesMetric.increment();
            }

            Slice sizeSlice = Slices.allocate(Integer.BYTES);
            sizeSlice.setInt(0, data.length());
            writeInternal(sizeSlice);
            writeInternal(data);

            currentFileSize += requiredPageStorageSize;
        }

        public synchronized ListenableFuture<Void> finish()
        {
            if (closed) {
                return immediateFailedFuture(new IllegalStateException("BufferedStorageWriter has already closed"));
            }

            stats.getFileSizeInBytes().add(currentFileSize);
            flushIfNeeded(true);
            if (writers.size() == 1) {
                return currentWriter.finish();
            }
            return asVoid(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::finish).collect(toImmutableList())));
        }

        public synchronized ListenableFuture<Void> abort()
        {
            if (closed) {
                return immediateVoidFuture();
            }
            closed = true;

            if (writers.size() == 1) {
                return currentWriter.abort();
            }
            return asVoid(Futures.allAsList(writers.stream().map(ExchangeStorageWriter::abort).collect(toImmutableList())));
        }

        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + estimatedSizeOf(writers, ExchangeStorageWriter::getRetainedSize);
        }

        @GuardedBy("this")
        private void setupWriterForNextPart()
        {
            currentWriter = exchangeStorage.createExchangeStorageWriter(
                    outputDirectory.resolve(partitionId + "_" + writers.size() + DATA_FILE_SUFFIX));
            writers.add(currentWriter);
        }

        @GuardedBy("this")
        private void writeInternal(Slice slice)
        {
            int position = 0;
            while (position < slice.length()) {
                if (currentBuffer == null) {
                    currentBuffer = bufferPool.take();
                    if (currentBuffer == null) {
                        // buffer pool is closed
                        return;
                    }
                }
                int writableBytes = min(currentBuffer.writableBytes(), slice.length() - position);
                currentBuffer.writeBytes(slice.getBytes(position, writableBytes));
                position += writableBytes;

                flushIfNeeded(false);
            }
        }

        @GuardedBy("this")
        private void flushIfNeeded(boolean finished)
        {
            SliceOutput buffer = currentBuffer;
            if (buffer != null && (!buffer.isWritable() || finished)) {
                if (!buffer.isWritable()) {
                    currentBuffer = null;
                }
                ListenableFuture<Void> writeFuture = currentWriter.write(buffer.slice());
                writeFuture.addListener(() -> bufferPool.offer(buffer), directExecutor());
                addExceptionCallback(writeFuture, throwable -> failure.compareAndSet(null, throwable));
            }
        }
    }

    @ThreadSafe
    private static class BufferPool
    {
        private static final int INSTANCE_SIZE = instanceSize(BufferPool.class);

        private final FileSystemExchangeStats stats;
        private final int maxNumBuffers;
        private final int writeBufferSize;
        private final long bufferRetainedSize;
        @GuardedBy("this")
        private final Queue<SliceOutput> freeBuffersQueue;
        @GuardedBy("this")
        private CompletableFuture<Void> blockedFuture = new CompletableFuture<>();
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private int numBuffersCreated;

        public BufferPool(FileSystemExchangeStats stats, int maxNumBuffers, int writeBufferSize)
        {
            this.stats = requireNonNull(stats, "stats is null");
            checkArgument(maxNumBuffers >= 1, "maxNumBuffers must be at least one");

            this.maxNumBuffers = maxNumBuffers;
            this.writeBufferSize = writeBufferSize;
            this.numBuffersCreated = 1;
            this.freeBuffersQueue = new ArrayDeque<>(maxNumBuffers);
            freeBuffersQueue.add(Slices.allocate(writeBufferSize).getOutput());
            this.bufferRetainedSize = freeBuffersQueue.peek().getRetainedSize();
        }

        public synchronized CompletableFuture<Void> isBlocked()
        {
            if (!hasFreeBuffers()) {
                if (blockedFuture.isDone()) {
                    blockedFuture = new CompletableFuture<>();
                    stats.getExchangeSinkBlocked().record(blockedFuture);
                }
                return blockedFuture;
            }
            return NOT_BLOCKED;
        }

        public synchronized SliceOutput take()
        {
            while (true) {
                if (closed) {
                    return null;
                }
                if (hasFreeBuffers()) {
                    return freeBuffersQueue.poll();
                }
                try {
                    wait();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }

        public void offer(SliceOutput buffer)
        {
            buffer.reset();

            CompletableFuture<Void> completableFuture;
            synchronized (this) {
                if (closed) {
                    return;
                }
                completableFuture = blockedFuture;
                freeBuffersQueue.add(buffer);
                notify();
            }

            completableFuture.complete(null);
        }

        public synchronized long getRetainedSize()
        {
            if (closed) {
                return INSTANCE_SIZE;
            }
            return INSTANCE_SIZE + numBuffersCreated * bufferRetainedSize;
        }

        public void close()
        {
            CompletableFuture<Void> completableFuture;
            synchronized (this) {
                if (closed) {
                    return;
                }
                closed = true;
                notifyAll();
                completableFuture = blockedFuture;
                freeBuffersQueue.clear();
            }

            completableFuture.complete(null);
        }

        @GuardedBy("this")
        private boolean hasFreeBuffers()
        {
            if (!freeBuffersQueue.isEmpty()) {
                return true;
            }
            if (numBuffersCreated < maxNumBuffers) {
                freeBuffersQueue.add(Slices.allocate(writeBufferSize).getOutput());
                numBuffersCreated++;
                return true;
            }
            return false;
        }
    }
}

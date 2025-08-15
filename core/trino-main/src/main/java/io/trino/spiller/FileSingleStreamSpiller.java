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
package io.trino.spiller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.units.DataSize;
import io.trino.annotation.NotThreadSafe;
import io.trino.execution.buffer.PageDeserializer;
import io.trino.execution.buffer.PageSerializer;
import io.trino.execution.buffer.PagesSerdeFactory;
import io.trino.execution.buffer.PagesSerdeUtil;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.SpillContext;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import jakarta.annotation.Nullable;

import javax.crypto.SecretKey;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class FileSingleStreamSpiller
        implements SingleStreamSpiller
{
    @VisibleForTesting
    static final int BUFFER_SIZE = 4 * 1024;
    static final int DEFAULT_SEGMENT_SIZE = 64 * 1024 * 1024; // 64 MB

    private final int segmentSize;

    private final List<FileHolder> targetFiles;
    private volatile long currentSegmentBytes;
    private volatile int currentFileIndex;

    private final Closer closer = Closer.create();
    private final PagesSerdeFactory serdeFactory;
    private volatile Optional<SecretKey> encryptionKey;
    private final boolean encrypted;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;

    private final ListeningExecutorService executor;

    private final AtomicBoolean writable = new AtomicBoolean(true);
    private final AtomicLong spilledPagesInMemorySize = new AtomicLong();
    private ListenableFuture<DataSize> spillInProgress = immediateFuture(DataSize.ofBytes(0L));

    private final Runnable fileSystemErrorHandler;

    public FileSingleStreamSpiller(
            PagesSerdeFactory serdeFactory,
            Optional<SecretKey> encryptionKey,
            ListeningExecutorService executor,
            List<Path> spillPaths,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext,
            Runnable fileSystemErrorHandler)
    {
        this(
                serdeFactory,
                encryptionKey,
                executor,
                spillPaths,
                spillerStats,
                spillContext,
                memoryContext,
                fileSystemErrorHandler,
                DEFAULT_SEGMENT_SIZE);
    }

    @VisibleForTesting
    public FileSingleStreamSpiller(
            PagesSerdeFactory serdeFactory,
            Optional<SecretKey> encryptionKey,
            ListeningExecutorService executor,
            List<Path> spillPaths,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext,
            Runnable fileSystemErrorHandler,
            int segmentSize)
    {
        this.serdeFactory = requireNonNull(serdeFactory, "serdeFactory is null");
        this.encryptionKey = requireNonNull(encryptionKey, "encryptionKey is null");
        this.encrypted = encryptionKey.isPresent();
        this.executor = requireNonNull(executor, "executor is null");
        this.spillerStats = requireNonNull(spillerStats, "spillerStats is null");
        this.localSpillContext = spillContext.newLocalSpillContext();
        this.memoryContext = requireNonNull(memoryContext, "memoryContext is null");
        // HACK!
        // The writePages() method is called in a separate thread pool and it's possible that
        // these spiller thread can run concurrently with the close() method.
        // Due to this race when the spiller thread is running, the driver thread:
        // 1. Can zero out the memory reservation even though the spiller thread physically holds onto that memory.
        // 2. Can close/delete the temp file(s) used for spilling, which doesn't have any visible side effects, but still not desirable.
        // To hack around the first issue we reserve the memory in the constructor and we release it in the close() method.
        // This means we start accounting for the memory before the spiller thread allocates it, and we release the memory reservation
        // before/after the spiller thread allocates that memory -- -- whether before or after depends on whether writePages() is in the
        // middle of execution when close() is called (note that this applies to both readPages() and writePages() methods).
        this.memoryContext.setBytes(BUFFER_SIZE);
        this.fileSystemErrorHandler = requireNonNull(fileSystemErrorHandler, "filesystemErrorHandler is null");
        this.segmentSize = segmentSize;
        requireNonNull(spillPaths, "spillPaths is null");
        checkState(!spillPaths.isEmpty(), "spillPaths is empty");
        try {
            ImmutableList.Builder<FileHolder> builder = ImmutableList.builder();
            for (Path path : spillPaths) {
                builder.add(closer.register(new FileHolder(Files.createTempFile(path, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX))));
            }
            this.targetFiles = builder.build();
        }
        catch (IOException e) {
            this.fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create spill file", e);
        }
    }

    @Override
    public ListenableFuture<DataSize> spill(Iterator<Page> pageIterator)
    {
        requireNonNull(pageIterator, "pageIterator is null");
        checkNoSpillInProgress();
        spillInProgress = Futures.submit(() -> writePages(pageIterator), executor);
        return spillInProgress;
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return spilledPagesInMemorySize.longValue();
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        checkState(writable.getAndSet(false), "Repeated reads are disallowed to prevent potential resource leaks");

        try {
            Optional<SecretKey> encryptionKey = this.encryptionKey;
            checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
            PageDeserializer deserializer = serdeFactory.createDeserializer(encryptionKey);
            this.encryptionKey = Optional.empty();

            List<Iterator<Iterator<Page>>> segmentsPerFile = new ArrayList<>();
            for (FileHolder file : targetFiles) {
                segmentsPerFile.add(readSegments(deserializer, file, closer));
            }

            int fileCount = segmentsPerFile.size();

            return new AbstractIterator<>()
            {
                int fileIndex;
                Iterator<Page> current = ImmutableList.<Page>of().iterator();

                @Override
                protected Page computeNext()
                {
                    while (true) {
                        if (current.hasNext()) {
                            return current.next();
                        }

                        int start = fileIndex;
                        boolean found = false;
                        do {
                            Iterator<Iterator<Page>> segments = segmentsPerFile.get(fileIndex);
                            if (segments.hasNext()) {
                                current = segments.next();
                                fileIndex = (fileIndex + 1) % fileCount;
                                found = true;
                                break;
                            }
                            fileIndex = (fileIndex + 1) % fileCount;
                        } while (fileIndex != start);

                        if (!found) {
                            return endOfData();
                        }
                    }
                }
            };
        }
        catch (IOException e) {
            fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
    }

    @Override
    public ListenableFuture<List<Page>> getAllSpilledPages()
    {
        checkNoSpillInProgress();
        checkState(writable.getAndSet(false), "Repeated reads are disallowed to prevent potential resource leaks");

        Optional<SecretKey> encryptionKey = this.encryptionKey;
        checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
        this.encryptionKey = Optional.empty();

        List<ListenableFuture<List<List<Page>>>> futures = new ArrayList<>();
        for (FileHolder file : targetFiles) {
            futures.add(executor.submit(() -> {
                PageDeserializer deserializer = serdeFactory.createDeserializer(encryptionKey);
                ImmutableList.Builder<List<Page>> segments = ImmutableList.builder();
                try (Closer closer = Closer.create()) {
                    Iterator<Iterator<Page>> segmentIterator = readSegments(deserializer, file, closer);
                    // materialize all segments from the file using executor thread
                    while (segmentIterator.hasNext()) {
                        segments.add(ImmutableList.copyOf(segmentIterator.next()));
                    }
                }
                return segments.build();
            }));
        }

        // Combine pages from all spill files according to the segment order.
        return Futures.transform(Futures.allAsList(futures), segmentsPerFile -> {
            ImmutableList.Builder<Page> builder = ImmutableList.builder();
            int fileCount = segmentsPerFile.size();
            List<Iterator<List<Page>>> iterators = new ArrayList<>(fileCount);
            for (List<List<Page>> segments : segmentsPerFile) {
                iterators.add(segments.iterator());
            }

            int fileIndex = 0;
            while (true) {
                boolean found = false;
                for (int i = 0; i < fileCount; i++) {
                    int index = (fileIndex + i) % fileCount;
                    Iterator<List<Page>> segments = iterators.get(index);
                    if (segments.hasNext()) {
                        segments.next().forEach(builder::add);
                        fileIndex = (index + 1) % fileCount;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
            }
            return builder.build();
        }, executor);
    }

    private DataSize writePages(Iterator<Page> pages)
    {
        checkState(writable.get(), "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");

        Optional<SecretKey> encryptionKey = this.encryptionKey;
        checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
        PageSerializer serializer = serdeFactory.createSerializer(encryptionKey);

        long spilledPagesBytes = 0;
        int fileIndex = currentFileIndex;

        try {
            while (pages.hasNext()) {
                long segmentBytes = currentSegmentBytes;
                try (SliceOutput out = newSliceOutput(fileIndex)) {
                    while (pages.hasNext()) {
                        Page page = pages.next();
                        long pageSizeInBytes = page.getSizeInBytes();
                        Slice serialized = serializer.serialize(page);
                        long serializedPageSize = serialized.length();

                        // check if the next page would exceed the segment size
                        if (targetFiles.size() > 1 && segmentBytes > 0 && segmentBytes + serializedPageSize > segmentSize) {
                            pages = Iterators.concat(ImmutableList.of(page).iterator(), pages);
                            break;
                        }

                        out.writeBytes(serialized);

                        segmentBytes += serializedPageSize;
                        spilledPagesBytes += pageSizeInBytes;

                        spilledPagesInMemorySize.addAndGet(pageSizeInBytes);
                        localSpillContext.updateBytes(serializedPageSize);
                        spillerStats.addToTotalSpilledBytes(serializedPageSize);

                        // check if the segment is full
                        if (targetFiles.size() > 1 && segmentBytes >= segmentSize) {
                            break;
                        }
                    }
                }

                currentSegmentBytes = segmentBytes;

                // switch to the next file if the segment is full or there are more pages to read
                if (targetFiles.size() > 1 && (segmentBytes >= segmentSize || pages.hasNext())) {
                    fileIndex = (fileIndex + 1) % targetFiles.size();
                    currentSegmentBytes = 0;
                }
            }

            currentFileIndex = fileIndex;
        }
        catch (UncheckedIOException | IOException e) {
            fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }

        return DataSize.ofBytes(spilledPagesBytes);
    }

    @Override
    public void close()
    {
        encryptionKey = Optional.empty();

        closer.register(localSpillContext);
        closer.register(() -> memoryContext.setBytes(0));
        try {
            closer.close();
        }
        catch (IOException e) {
            fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to close spiller", e);
        }
    }

    private void checkNoSpillInProgress()
    {
        checkState(spillInProgress.isDone(), "spill in progress");
    }

    private SliceOutput newSliceOutput(int fileIndex)
            throws IOException
    {
        return new OutputStreamSliceOutput(targetFiles.get(fileIndex).newOutputStream(APPEND), BUFFER_SIZE);
    }

    /**
     * Returns an iterator producing page iterators for each segment in the given file.
     * Segments are split based on the configured {@code segmentSize} so that only
     * one segment of pages needs to be materialized at a time. The returned
     * iterators lazily deserialize pages from the file.
     */
    private Iterator<Iterator<Page>> readSegments(PageDeserializer deserializer, FileHolder file, Closer closer)
            throws IOException
    {
        InputStream input = closer.register(file.newInputStream());
        Iterator<Slice> serializedPages = closeWhenExhausted(PagesSerdeUtil.readSerializedPages(input), input);

        return new AbstractIterator<>()
        {
            @Nullable
            Slice nextSlice;

            @Override
            protected Iterator<Page> computeNext()
            {
                if (nextSlice == null && !serializedPages.hasNext()) {
                    return endOfData();
                }

                Slice first = nextSlice != null ? nextSlice : serializedPages.next();
                nextSlice = null;

                return new AbstractIterator<>()
                {
                    long bytes;
                    Slice slice = first;
                    boolean finished;

                    @Override
                    protected Page computeNext()
                    {
                        if (finished) {
                            return endOfData();
                        }

                        if (slice == null) {
                            if (!serializedPages.hasNext()) {
                                return endOfData();
                            }
                            slice = serializedPages.next();
                        }

                        // check if the next page would exceed the segment size
                        long size = slice.length();
                        if (targetFiles.size() > 1 && bytes > 0 && bytes + size > segmentSize) {
                            nextSlice = slice;
                            slice = null;
                            finished = true;
                            return endOfData();
                        }

                        bytes += size;
                        Page page = deserializer.deserialize(slice);
                        slice = null;
                        // If the segment is full, we finish reading it
                        if (targetFiles.size() > 1 && bytes >= segmentSize) {
                            finished = true;
                        }
                        return page;
                    }
                };
            }
        };
    }

    private static <T> Iterator<T> closeWhenExhausted(Iterator<T> iterator, Closeable resource)
    {
        requireNonNull(iterator, "iterator is null");
        requireNonNull(resource, "resource is null");

        return new AbstractIterator<>()
        {
            @Override
            protected T computeNext()
            {
                if (iterator.hasNext()) {
                    return iterator.next();
                }
                try {
                    resource.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return endOfData();
            }
        };
    }
}

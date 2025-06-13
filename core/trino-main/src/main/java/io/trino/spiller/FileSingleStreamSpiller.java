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

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slice;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_PREFIX;
import static io.trino.spiller.FileSingleStreamSpillerFactory.SPILL_FILE_SUFFIX;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class FileSingleStreamSpiller
        implements SingleStreamSpiller
{
    private final List<SpillFile> spillFiles;
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
        requireNonNull(spillPaths, "spillPaths is null");
        checkArgument(!spillPaths.isEmpty(), "spillPaths is empty");

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
        this.memoryContext.setBytes((long) SpillFile.BUFFER_SIZE * spillPaths.size());
        this.fileSystemErrorHandler = requireNonNull(fileSystemErrorHandler, "filesystemErrorHandler is null");
        try {
            ImmutableList.Builder<SpillFile> builder = ImmutableList.builderWithExpectedSize(spillPaths.size());
            for (Path path : spillPaths) {
                builder.add(closer.register(new SpillFile(Files.createTempFile(path, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX))));
            }
            this.spillFiles = builder.build();
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

            int fileCount = spillFiles.size();
            List<Iterator<Page>> iterators = new ArrayList<>(fileCount);
            for (SpillFile file : spillFiles) {
                iterators.add(readFilePages(deserializer, file, closer));
            }

            return new AbstractIterator<>()
            {
                int fileIndex;

                @Override
                protected Page computeNext()
                {
                    Iterator<Page> iterator = iterators.get(fileIndex);
                    if (!iterator.hasNext()) {
                        checkAllIteratorsExhausted(iterators);
                        return endOfData();
                    }

                    Page page = iterator.next();
                    fileIndex = (fileIndex + 1) % fileCount;
                    return page;
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

        List<ListenableFuture<List<Page>>> futures = new ArrayList<>();
        for (SpillFile file : spillFiles) {
            futures.add(executor.submit(() -> {
                PageDeserializer deserializer = serdeFactory.createDeserializer(encryptionKey);
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                try (Closer closer = Closer.create()) {
                    readFilePages(deserializer, file, closer).forEachRemaining(pages::add);
                }
                return pages.build();
            }));
        }

        // Combine pages from all spill files according to the round-robin order.
        return Futures.transform(Futures.allAsList(futures), pagesPerFile -> {
            ImmutableList.Builder<Page> builder = ImmutableList.builderWithExpectedSize(pagesPerFile.stream().mapToInt(List::size).sum());
            int fileCount = spillFiles.size();

            List<Iterator<Page>> iterators = new ArrayList<>(fileCount);
            for (List<Page> pages : pagesPerFile) {
                iterators.add(pages.iterator());
            }

            int fileIndex = 0;
            while (iterators.get(fileIndex).hasNext()) {
                builder.add(iterators.get(fileIndex).next());
                fileIndex = (fileIndex + 1) % fileCount;
            }
            checkAllIteratorsExhausted(iterators);
            return builder.build();
        }, executor);
    }

    private static void checkAllIteratorsExhausted(List<Iterator<Page>> iterators)
    {
        iterators.forEach(iterator -> checkState(!iterator.hasNext(), "spill file iterator not fully consumed"));
    }

    private DataSize writePages(Iterator<Page> pages)
    {
        checkState(writable.get(), "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");

        Optional<SecretKey> encryptionKey = this.encryptionKey;
        checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
        PageSerializer serializer = serdeFactory.createSerializer(encryptionKey);

        long spilledPagesBytes = 0;
        int fileIndex = currentFileIndex;
        int fileCount = spillFiles.size();

        try {
            while (pages.hasNext()) {
                Page page = pages.next();
                long pageSizeInBytes = page.getSizeInBytes();
                Slice serialized = serializer.serialize(page);
                long serializedPageSize = serialized.length();

                spillFiles.get(fileIndex).writeBytes(serialized);

                spilledPagesBytes += pageSizeInBytes;

                spilledPagesInMemorySize.addAndGet(pageSizeInBytes);
                localSpillContext.updateBytes(serializedPageSize);
                spillerStats.addToTotalSpilledBytes(serializedPageSize);

                fileIndex = (fileIndex + 1) % fileCount;
            }

            currentFileIndex = fileIndex;

            for (SpillFile file : spillFiles) {
                file.closeOutput();
            }
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

    /**
     * Returns an iterator that exposes all pages stored in the given file.
     * Pages are lazily deserialized as the iterator is consumed.
     */
    private Iterator<Page> readFilePages(PageDeserializer deserializer, SpillFile file, Closer closer)
            throws IOException
    {
        InputStream input = closer.register(file.newInputStream());
        return transform(closeWhenExhausted(PagesSerdeUtil.readSerializedPages(input), input), deserializer::deserialize);
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

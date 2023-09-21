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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
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
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
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

    private final FileHolder targetFile;
    private final Closer closer = Closer.create();
    private final PagesSerdeFactory serdeFactory;
    private volatile Optional<SecretKey> encryptionKey;
    private final boolean encrypted;
    private final SpillerStats spillerStats;
    private final SpillContext localSpillContext;
    private final LocalMemoryContext memoryContext;

    private final ListeningExecutorService executor;

    private boolean writable = true;
    private long spilledPagesInMemorySize;
    private ListenableFuture<Void> spillInProgress = immediateVoidFuture();

    private final Runnable fileSystemErrorHandler;

    public FileSingleStreamSpiller(
            PagesSerdeFactory serdeFactory,
            Optional<SecretKey> encryptionKey,
            ListeningExecutorService executor,
            Path spillPath,
            SpillerStats spillerStats,
            SpillContext spillContext,
            LocalMemoryContext memoryContext,
            Runnable fileSystemErrorHandler)
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
        try {
            this.targetFile = closer.register(new FileHolder(Files.createTempFile(spillPath, SPILL_FILE_PREFIX, SPILL_FILE_SUFFIX)));
        }
        catch (IOException e) {
            this.fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to create spill file", e);
        }
    }

    @Override
    public ListenableFuture<Void> spill(Iterator<Page> pageIterator)
    {
        requireNonNull(pageIterator, "pageIterator is null");
        checkNoSpillInProgress();
        spillInProgress = Futures.submit(() -> writePages(pageIterator), executor);
        return spillInProgress;
    }

    @Override
    public long getSpilledPagesInMemorySize()
    {
        return spilledPagesInMemorySize;
    }

    @Override
    public Iterator<Page> getSpilledPages()
    {
        checkNoSpillInProgress();
        return readPages();
    }

    @Override
    public ListenableFuture<List<Page>> getAllSpilledPages()
    {
        return executor.submit(() -> ImmutableList.copyOf(getSpilledPages()));
    }

    private void writePages(Iterator<Page> pageIterator)
    {
        checkState(writable, "Spilling no longer allowed. The spiller has been made non-writable on first read for subsequent reads to be consistent");

        Optional<SecretKey> encryptionKey = this.encryptionKey;
        checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
        PageSerializer serializer = serdeFactory.createSerializer(encryptionKey);
        try (SliceOutput output = new OutputStreamSliceOutput(targetFile.newOutputStream(APPEND), BUFFER_SIZE)) {
            while (pageIterator.hasNext()) {
                Page page = pageIterator.next();
                spilledPagesInMemorySize += page.getSizeInBytes();
                Slice serializedPage = serializer.serialize(page);
                long pageSize = serializedPage.length();
                localSpillContext.updateBytes(pageSize);
                spillerStats.addToTotalSpilledBytes(pageSize);
                output.writeBytes(serializedPage);
            }
        }
        catch (UncheckedIOException | IOException e) {
            fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to spill pages", e);
        }
    }

    private Iterator<Page> readPages()
    {
        checkState(writable, "Repeated reads are disallowed to prevent potential resource leaks");
        writable = false;

        try {
            Optional<SecretKey> encryptionKey = this.encryptionKey;
            checkState(encrypted == encryptionKey.isPresent(), "encryptionKey has been discarded");
            PageDeserializer deserializer = serdeFactory.createDeserializer(encryptionKey);
            // encryption key is safe to discard since it now belongs to the PageDeserializer and repeated reads are disallowed
            this.encryptionKey = Optional.empty();
            InputStream input = closer.register(targetFile.newInputStream());
            Iterator<Page> pages = PagesSerdeUtil.readPages(deserializer, input);
            return closeWhenExhausted(pages, input);
        }
        catch (IOException e) {
            fileSystemErrorHandler.run();
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Failed to read spilled pages", e);
        }
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

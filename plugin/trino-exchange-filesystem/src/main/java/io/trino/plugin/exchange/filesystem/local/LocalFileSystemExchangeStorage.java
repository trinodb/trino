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
package io.trino.plugin.exchange.filesystem.local;

import com.google.common.collect.ImmutableList;
import com.google.common.io.MoreFiles;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.trino.annotation.NotThreadSafe;
import io.trino.plugin.exchange.filesystem.ExchangeSourceFile;
import io.trino.plugin.exchange.filesystem.ExchangeStorageReader;
import io.trino.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.trino.plugin.exchange.filesystem.FileStatus;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeStorage;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.immediateVoidFuture;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Math.toIntExact;
import static java.nio.file.Files.createFile;
import static java.util.Objects.requireNonNull;

public class LocalFileSystemExchangeStorage
        implements FileSystemExchangeStorage
{
    private static final int BUFFER_SIZE_IN_BYTES = toIntExact(DataSize.of(4, KILOBYTE).toBytes());

    @Override
    public void createDirectories(URI dir)
            throws IOException
    {
        Files.createDirectories(Paths.get(dir.getPath()));
    }

    @Override
    public ExchangeStorageReader createExchangeStorageReader(List<ExchangeSourceFile> sourceFiles, int maxPageStorageSize)
    {
        return new LocalExchangeStorageReader(sourceFiles);
    }

    @Override
    public ExchangeStorageWriter createExchangeStorageWriter(URI file)
    {
        return new LocalExchangeStorageWriter(file);
    }

    @Override
    public ListenableFuture<Void> createEmptyFile(URI file)
    {
        try {
            createFile(Paths.get(file.getPath()));
        }
        catch (IOException | RuntimeException e) {
            return immediateFailedFuture(e);
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<Void> deleteRecursively(List<URI> directories)
    {
        for (URI dir : directories) {
            try {
                MoreFiles.deleteRecursively(Paths.get(dir.getPath()), ALLOW_INSECURE);
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
        }
        return immediateVoidFuture();
    }

    @Override
    public ListenableFuture<List<FileStatus>> listFilesRecursively(URI dir)
    {
        ImmutableList.Builder<FileStatus> builder = ImmutableList.builder();
        try {
            try (Stream<Path> paths = Files.walk(Paths.get(dir.getPath()))) {
                for (Path file : paths.filter(Files::isRegularFile).collect(toImmutableList())) {
                    builder.add(new FileStatus(file.toUri().toString(), Files.size(file)));
                }
            }
        }
        catch (IOException e) {
            return immediateFailedFuture(e);
        }
        return immediateFuture(builder.build());
    }

    @Override
    public int getWriteBufferSize()
    {
        return BUFFER_SIZE_IN_BYTES;
    }

    @Override
    public void close()
    {
    }

    @ThreadSafe
    private static class LocalExchangeStorageReader
            implements ExchangeStorageReader
    {
        private static final int INSTANCE_SIZE = instanceSize(LocalExchangeStorageReader.class);

        @GuardedBy("this")
        private final Queue<ExchangeSourceFile> sourceFiles;

        @GuardedBy("this")
        private InputStreamSliceInput sliceInput;
        @GuardedBy("this")
        private boolean closed;

        public LocalExchangeStorageReader(List<ExchangeSourceFile> sourceFiles)
        {
            this.sourceFiles = new ArrayDeque<>(requireNonNull(sourceFiles, "sourceFiles is null"));
        }

        @Override
        public synchronized Slice read()
                throws IOException
        {
            if (closed) {
                return null;
            }

            if (sliceInput != null) {
                if (sliceInput.isReadable()) {
                    return sliceInput.readSlice(sliceInput.readInt());
                }
                else {
                    sliceInput.close();
                }
            }

            ExchangeSourceFile sourceFile = sourceFiles.poll();
            if (sourceFile == null) {
                close();
                return null;
            }

            sliceInput = getSliceInput(sourceFile);
            return sliceInput.readSlice(sliceInput.readInt());
        }

        @Override
        public ListenableFuture<Void> isBlocked()
        {
            return immediateVoidFuture();
        }

        @Override
        public synchronized long getRetainedSize()
        {
            return INSTANCE_SIZE + (sliceInput == null ? 0 : sliceInput.getRetainedSize());
        }

        @Override
        public synchronized boolean isFinished()
        {
            return closed;
        }

        @Override
        public synchronized void close()
        {
            if (closed) {
                return;
            }
            closed = true;
            if (sliceInput != null) {
                sliceInput.close();
                sliceInput = null;
            }
        }

        private InputStreamSliceInput getSliceInput(ExchangeSourceFile sourceFile)
                throws FileNotFoundException
        {
            return new InputStreamSliceInput(new FileInputStream(Paths.get(sourceFile.getFileUri()).toFile()), BUFFER_SIZE_IN_BYTES);
        }
    }

    @NotThreadSafe
    private static class LocalExchangeStorageWriter
            implements ExchangeStorageWriter
    {
        private static final int INSTANCE_SIZE = instanceSize(LocalExchangeStorageWriter.class);

        private final OutputStream outputStream;

        public LocalExchangeStorageWriter(URI file)
        {
            try {
                this.outputStream = new FileOutputStream(Paths.get(file.getPath()).toFile());
            }
            catch (FileNotFoundException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public ListenableFuture<Void> write(Slice slice)
        {
            try {
                outputStream.write(slice.getBytes());
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> finish()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public ListenableFuture<Void> abort()
        {
            try {
                outputStream.close();
            }
            catch (IOException | RuntimeException e) {
                return immediateFailedFuture(e);
            }
            return immediateVoidFuture();
        }

        @Override
        public long getRetainedSize()
        {
            return INSTANCE_SIZE;
        }
    }
}

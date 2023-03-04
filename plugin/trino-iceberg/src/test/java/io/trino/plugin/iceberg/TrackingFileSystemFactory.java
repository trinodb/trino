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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.SeekableInputStream;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.spi.security.ConnectorIdentity;

import javax.annotation.concurrent.Immutable;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.INPUT_FILE_EXISTS;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.plugin.iceberg.TrackingFileSystemFactory.OperationType.OUTPUT_FILE_LOCATION;
import static java.util.Objects.requireNonNull;

public class TrackingFileSystemFactory
        implements TrinoFileSystemFactory
{
    public enum OperationType
    {
        INPUT_FILE_GET_LENGTH,
        INPUT_FILE_NEW_STREAM,
        INPUT_FILE_EXISTS,
        OUTPUT_FILE_CREATE,
        OUTPUT_FILE_CREATE_OR_OVERWRITE,
        OUTPUT_FILE_LOCATION,
        OUTPUT_FILE_TO_INPUT_FILE,
    }

    private final AtomicInteger fileId = new AtomicInteger();
    private final TrinoFileSystemFactory delegate;

    private final Map<OperationContext, Integer> operationCounts = new ConcurrentHashMap<>();

    public TrackingFileSystemFactory(TrinoFileSystemFactory delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    public Map<OperationContext, Integer> getOperationCounts()
    {
        return ImmutableMap.copyOf(operationCounts);
    }

    public void reset()
    {
        operationCounts.clear();
    }

    private void increment(String path, int fileId, OperationType operationType)
    {
        OperationContext context = new OperationContext(path, fileId, operationType);
        operationCounts.merge(context, 1, Math::addExact);    // merge is atomic for ConcurrentHashMap
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new TrackingFileSystem(delegate.create(identity), this::increment);
    }

    private interface Tracker
    {
        void track(String path, int fileId, OperationType operationType);
    }

    private class TrackingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem delegate;
        private final Tracker tracker;

        private TrackingFileSystem(TrinoFileSystem delegate, Tracker tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public TrinoInputFile newInputFile(String path)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingInputFile(
                    delegate.newInputFile(path),
                    operation -> tracker.track(path, nextId, operation));
        }

        @Override
        public TrinoInputFile newInputFile(String path, long length)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingInputFile(
                    delegate.newInputFile(path, length),
                    operation -> tracker.track(path, nextId, operation));
        }

        @Override
        public TrinoOutputFile newOutputFile(String path)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingOutputFile(
                    delegate.newOutputFile(path),
                    operationType -> tracker.track(path, nextId, operationType));
        }

        @Override
        public void deleteFile(String path)
                throws IOException
        {
            delegate.deleteFile(path);
        }

        @Override
        public void deleteFiles(Collection<String> paths)
                throws IOException
        {
            delegate.deleteFiles(paths);
        }

        @Override
        public void deleteDirectory(String path)
                throws IOException
        {
            delegate.deleteDirectory(path);
        }

        @Override
        public void renameFile(String source, String target)
                throws IOException
        {
            delegate.renameFile(source, target);
        }

        @Override
        public FileIterator listFiles(String path)
                throws IOException
        {
            return delegate.listFiles(path);
        }
    }

    private static class TrackingInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingInputFile(TrinoInputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public long length()
                throws IOException
        {
            tracker.accept(INPUT_FILE_GET_LENGTH);
            return delegate.length();
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            tracker.accept(INPUT_FILE_NEW_STREAM);
            return delegate.newInput();
        }

        @Override
        public SeekableInputStream newStream()
                throws IOException
        {
            tracker.accept(INPUT_FILE_NEW_STREAM);
            return delegate.newStream();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            tracker.accept(INPUT_FILE_EXISTS);
            return delegate.exists();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            return delegate.lastModified();
        }

        @Override
        public String location()
        {
            return delegate.location();
        }
    }

    private static class TrackingOutputFile
            implements TrinoOutputFile
    {
        private final TrinoOutputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingOutputFile(TrinoOutputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delete is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public OutputStream create(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            tracker.accept(OUTPUT_FILE_CREATE);
            return delegate.create(memoryContext);
        }

        @Override
        public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
                throws IOException
        {
            tracker.accept(OUTPUT_FILE_CREATE_OR_OVERWRITE);
            return delegate.createOrOverwrite(memoryContext);
        }

        @Override
        public String location()
        {
            tracker.accept(OUTPUT_FILE_LOCATION);
            return delegate.location();
        }
    }

    @Immutable
    public static class OperationContext
    {
        private final String filePath;
        private final int fileId;
        private final OperationType operationType;

        public OperationContext(String filePath, int fileId, OperationType operationType)
        {
            this.filePath = requireNonNull(filePath, "filePath is null");
            this.fileId = fileId;
            this.operationType = requireNonNull(operationType, "operationType is null");
        }

        public String getFilePath()
        {
            return filePath;
        }

        public int getFileId()
        {
            return fileId;
        }

        public OperationType getOperationType()
        {
            return operationType;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            OperationContext that = (OperationContext) o;
            return Objects.equals(filePath, that.filePath)
                    && fileId == that.fileId
                    && operationType == that.operationType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(filePath, fileId, operationType);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("path", filePath)
                    .add("fileId", fileId)
                    .add("operation", operationType)
                    .toString();
        }
    }
}

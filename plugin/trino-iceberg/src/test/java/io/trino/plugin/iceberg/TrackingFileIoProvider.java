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
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

import javax.annotation.concurrent.Immutable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_EXISTS;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.INPUT_FILE_NEW_STREAM;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.OUTPUT_FILE_CREATE;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.OUTPUT_FILE_CREATE_OR_OVERWRITE;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.OUTPUT_FILE_LOCATION;
import static io.trino.plugin.iceberg.TrackingFileIoProvider.OperationType.OUTPUT_FILE_TO_INPUT_FILE;
import static java.util.Objects.requireNonNull;

public class TrackingFileIoProvider
        implements FileIoProvider
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
    private final FileIoProvider delegate;

    private final Map<OperationContext, Integer> operationCounts = new ConcurrentHashMap<>();

    public TrackingFileIoProvider(FileIoProvider delegate)
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
    public FileIO createFileIo(HdfsContext hdfsContext, String queryId)
    {
        return new TrackingFileIo(
                delegate.createFileIo(hdfsContext, queryId),
                this::increment);
    }

    private interface Tracker
    {
        void track(String path, int fileId, OperationType operationType);
    }

    private class TrackingFileIo
            implements FileIO
    {
        private final FileIO delegate;
        private final Tracker tracker;

        public TrackingFileIo(FileIO delegate, Tracker tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public InputFile newInputFile(String path)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingInputFile(
                    delegate.newInputFile(path),
                    operation -> tracker.track(path, nextId, operation));
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            int nextId = fileId.incrementAndGet();
            return new TrackingOutputFile(
                    delegate.newOutputFile(path),
                    operationType -> tracker.track(path, nextId, operationType));
        }

        @Override
        public void deleteFile(String path)
        {
            delegate.deleteFile(path);  // TODO: track delete files calls
        }
    }

    private static class TrackingInputFile
            implements InputFile
    {
        private final InputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingInputFile(InputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public long getLength()
        {
            tracker.accept(INPUT_FILE_GET_LENGTH);
            return delegate.getLength();
        }

        @Override
        public SeekableInputStream newStream()
        {
            tracker.accept(INPUT_FILE_NEW_STREAM);
            return delegate.newStream();
        }

        @Override
        public String location()
        {
            return delegate.location();
        }

        @Override
        public boolean exists()
        {
            tracker.accept(INPUT_FILE_EXISTS);
            return delegate.exists();
        }
    }

    private static class TrackingOutputFile
            implements OutputFile
    {
        private final OutputFile delegate;
        private final Consumer<OperationType> tracker;

        public TrackingOutputFile(OutputFile delegate, Consumer<OperationType> tracker)
        {
            this.delegate = requireNonNull(delegate, "delete is null");
            this.tracker = requireNonNull(tracker, "tracker is null");
        }

        @Override
        public PositionOutputStream create()
        {
            tracker.accept(OUTPUT_FILE_CREATE);
            return delegate.create();
        }

        @Override
        public PositionOutputStream createOrOverwrite()
        {
            tracker.accept(OUTPUT_FILE_CREATE_OR_OVERWRITE);
            return delegate.createOrOverwrite();
        }

        @Override
        public String location()
        {
            tracker.accept(OUTPUT_FILE_LOCATION);
            return delegate.location();
        }

        @Override
        public InputFile toInputFile()
        {
            tracker.accept(OUTPUT_FILE_TO_INPUT_FILE);
            return delegate.toInputFile();
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

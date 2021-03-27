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
package io.trino.plugin.iceberg.testing;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.iceberg.FileIoProvider;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.Immutable;
import javax.inject.Inject;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_EXISTS;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_GET_LENGTH;
import static io.trino.plugin.iceberg.testing.TrackingFileIoProvider.OperationType.INPUT_FILE_NEW_STREAM;
import static java.util.Objects.requireNonNull;

@VisibleForTesting
public class TrackingFileIoProvider
        implements FileIoProvider
{
    public enum OperationType
    {
        INPUT_FILE_GET_LENGTH,
        INPUT_FILE_NEW_STREAM,
        INPUT_FILE_EXISTS,
    }

    private final AtomicInteger fileId = new AtomicInteger();
    private final FileIoProvider delegate;

    private final Map<OperationContext, Integer> operationCounts = new ConcurrentHashMap<>();

    @Inject
    public TrackingFileIoProvider(@ForTrackingFileIoProvider FileIoProvider delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Managed
    public Map<OperationContext, Integer> getOperationCounts()
    {
        return ImmutableMap.copyOf(operationCounts);
    }

    private void increment(String queryId, String path, int fileId, OperationType operationType)
    {
        OperationContext context = new OperationContext(queryId, path, fileId, operationType);
        operationCounts.merge(context, 1, Math::addExact);    // merge is atomic for ConcurrentHashMap
    }

    @Override
    public FileIO createFileIo(HdfsContext hdfsContext, String queryId)
    {
        return new TrackingFileIo(
                delegate.createFileIo(hdfsContext, queryId),
                (path, fileId, operationType) -> increment(queryId, path, fileId, operationType));
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
            return delegate.newOutputFile(path);  // TODO: track output file calls
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

    @Immutable
    public static class OperationContext
    {
        private final String queryId;
        private final String filePath;
        private final int fileId;
        private final OperationType operationType;

        public OperationContext(String queryId, String filePath, int fileId, OperationType operationType)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.filePath = requireNonNull(filePath, "filePath is null");
            this.fileId = fileId;
            this.operationType = requireNonNull(operationType, "operationType is null");
        }

        public String getQueryId()
        {
            return queryId;
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
            return Objects.equals(queryId, that.queryId)
                && Objects.equals(filePath, that.filePath)
                && fileId == that.fileId
                && operationType == that.operationType;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(queryId, filePath, fileId, operationType);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("queryId", queryId)
                    .add("path", filePath)
                    .add("fileId", fileId)
                    .add("operation", operationType)
                    .toString();
        }
    }
}

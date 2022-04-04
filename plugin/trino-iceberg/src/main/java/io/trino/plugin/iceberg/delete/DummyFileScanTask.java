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
package io.trino.plugin.iceberg.delete;

import com.google.common.collect.ImmutableList;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataTask;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.expressions.Expression;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

// TODO: This wrapper is necessary until the constructors of the Iceberg DeleteFilter are made more specific
// Remove after upgrading to Iceberg with https://github.com/apache/iceberg/pull/4381
public class DummyFileScanTask
        implements FileScanTask
{
    private final DataFile file;
    private final List<DeleteFile> deletes;

    public DummyFileScanTask(String path, List<TrinoDeleteFile> deletes)
    {
        requireNonNull(path, "path is null");
        this.file = new DummyDataFile(path);
        this.deletes = ImmutableList.copyOf(requireNonNull(deletes, "deletes is null"));
    }

    @Override
    public DataFile file()
    {
        return file;
    }

    @Override
    public List<DeleteFile> deletes()
    {
        return deletes;
    }

    @Override
    public PartitionSpec spec()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long start()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long length()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Expression residual()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterable<FileScanTask> split(long l)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFileScanTask()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileScanTask asFileScanTask()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDataTask()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataTask asDataTask()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public CombinedScanTask asCombinedScanTask()
    {
        throw new UnsupportedOperationException();
    }

    private static class DummyDataFile
            implements DataFile
    {
        private final String path;

        private DummyDataFile(String path)
        {
            this.path = requireNonNull(path, "path is null");
        }

        @Override
        public Long pos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int specId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public CharSequence path()
        {
            return path;
        }

        @Override
        public FileFormat format()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public StructLike partition()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long recordCount()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long fileSizeInBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Long> columnSizes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Long> valueCounts()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Long> nullValueCounts()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, Long> nanValueCounts()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, ByteBuffer> lowerBounds()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Integer, ByteBuffer> upperBounds()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ByteBuffer keyMetadata()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Long> splitOffsets()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataFile copy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public DataFile copyWithoutStats()
        {
            throw new UnsupportedOperationException();
        }
    }
}

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
package io.trino.plugin.hidden.partitioning;

import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;

import java.nio.ByteBuffer;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Dummy implementation that wraps a single Hive partition.
 * Used to filter Hive partitions using {@link HiddenPartitioningPartitionSpec} transforms
 */
public class HiddenPartitioningManifestFile
        implements ManifestFile
{
    private final List<PartitionFieldSummary> partitions;

    public HiddenPartitioningManifestFile(List<PartitionFieldSummary> partitions)
    {
        this.partitions = requireNonNull(partitions, "partitions is null");
    }

    @Override
    public String path()
    {
        return null;
    }

    @Override
    public long length()
    {
        return 0;
    }

    @Override
    public int partitionSpecId()
    {
        return 0;
    }

    @Override
    public ManifestContent content()
    {
        return null;
    }

    @Override
    public long sequenceNumber()
    {
        return 0;
    }

    @Override
    public long minSequenceNumber()
    {
        return 0;
    }

    @Override
    public Long snapshotId()
    {
        return null;
    }

    @Override
    public Integer addedFilesCount()
    {
        return null;
    }

    @Override
    public Long addedRowsCount()
    {
        return null;
    }

    @Override
    public Integer existingFilesCount()
    {
        return null;
    }

    @Override
    public Long existingRowsCount()
    {
        return null;
    }

    @Override
    public Integer deletedFilesCount()
    {
        return null;
    }

    @Override
    public Long deletedRowsCount()
    {
        return null;
    }

    @Override
    public List<PartitionFieldSummary> partitions()
    {
        return partitions;
    }

    @Override
    public ManifestFile copy()
    {
        return null;
    }

    public static class TrinoPartitionFieldSummary
            implements ManifestFile.PartitionFieldSummary
    {
        private final boolean containsNull;
        private final ByteBuffer lowerBound;
        private final ByteBuffer upperBound;

        public TrinoPartitionFieldSummary(boolean containsNull, ByteBuffer lowerBound, ByteBuffer upperBound)
        {
            this.containsNull = containsNull;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        @Override
        public boolean containsNull()
        {
            return containsNull;
        }

        @Override
        public ByteBuffer lowerBound()
        {
            return lowerBound;
        }

        @Override
        public ByteBuffer upperBound()
        {
            return upperBound;
        }

        @Override
        public ManifestFile.PartitionFieldSummary copy()
        {
            return this;
        }
    }
}

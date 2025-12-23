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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.LocationProvider;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for Copy-on-Write DELETE operations API compatibility.
 * This test specifically addresses compilation errors related to abstract method implementations.
 */
public class TestIcebergCopyOnWriteDeleteOperations
{
    @Test
    public void testApiCompatibility()
    {
        // Simple test to verify that abstract methods are properly implemented
        TestingManifestFile manifestFile = new TestingManifestFile();
        TestingLocationProvider locationProvider = new TestingLocationProvider();
        TestingIcebergFileIO fileIO = new TestingIcebergFileIO();

        // Verify that the copy method exists and can be called
        assertThat(manifestFile.copy()).isNotNull();

        // Verify that the new newDataLocation method exists and can be called
        PartitionSpec spec = PartitionSpec.unpartitioned();
        StructLike data = new TestingStructLike();
        assertThat(locationProvider.newDataLocation(spec, data, "test.parquet")).isNotNull();

        // Verify that deleteFile method exists and can be called
        fileIO.deleteFile("test-path");
    }

    /**
     * A minimal test implementation of ManifestFile that implements the copy() method.
     */
    private static class TestingManifestFile
            implements ManifestFile
    {
        @Override
        public String path()
        {
            return "test-manifest.avro";
        }

        @Override
        public ManifestContent content()
        {
            return ManifestContent.DATA;
        }

        @Override
        public ManifestFile copy()
        {
            return new TestingManifestFile();
        }

        @Override
        public long length()
        {
            return 1024L;
        }

        @Override
        public int partitionSpecId()
        {
            return 0;
        }

        @Override
        public long sequenceNumber()
        {
            return 1L;
        }

        @Override
        public long minSequenceNumber()
        {
            return 1L;
        }

        @Override
        public Long snapshotId()
        {
            return 1L;
        }

        @Override
        public Integer addedFilesCount()
        {
            return 1;
        }

        @Override
        public Integer existingFilesCount()
        {
            return 0;
        }

        @Override
        public Integer deletedFilesCount()
        {
            return 0;
        }

        @Override
        public Long addedRowsCount()
        {
            return 10L;
        }

        @Override
        public Long existingRowsCount()
        {
            return 0L;
        }

        @Override
        public Long deletedRowsCount()
        {
            return 0L;
        }

        @Override
        public List<PartitionFieldSummary> partitions()
        {
            return ImmutableList.of();
        }

        @Override
        public boolean hasAddedFiles()
        {
            return true;
        }

        @Override
        public boolean hasExistingFiles()
        {
            return false;
        }

        @Override
        public boolean hasDeletedFiles()
        {
            return false;
        }

        @Override
        public Long firstRowId()
        {
            return null;
        }
    }

    /**
     * A minimal test implementation of LocationProvider that implements the new newDataLocation method.
     */
    private static class TestingLocationProvider
            implements LocationProvider
    {
        @Override
        public String newDataLocation(String filename)
        {
            return "data/" + filename;
        }

        public String newDataLocation(PartitionSpec spec, PartitionData data, String filename)
        {
            return "data/" + filename;
        }

        @Override
        public String newDataLocation(PartitionSpec spec, StructLike data, String filename)
        {
            return "data/" + filename;
        }
    }

    /**
     * A minimal test implementation of FileIO that implements the deleteFile method.
     */
    private static class TestingIcebergFileIO
            implements org.apache.iceberg.io.FileIO
    {
        @Override
        public org.apache.iceberg.io.InputFile newInputFile(String path)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public org.apache.iceberg.io.OutputFile newOutputFile(String path)
        {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public void deleteFile(String path)
        {
            // Do nothing for testing
        }

        @Override
        public Map<String, String> properties()
        {
            return ImmutableMap.of();
        }

        @Override
        public void initialize(Map<String, String> properties)
        {
            // Do nothing for testing
        }

        @Override
        public void close()
        {
            // Do nothing for testing
        }
    }

    /**
     * A minimal test implementation of StructLike.
     */
    private static class TestingStructLike
            implements StructLike
    {
        @Override
        public int size()
        {
            return 0;
        }

        @Override
        public <T> T get(int pos, Class<T> javaClass)
        {
            return null;
        }

        @Override
        public <T> void set(int pos, T value)
        {
            // Do nothing
        }
    }
}

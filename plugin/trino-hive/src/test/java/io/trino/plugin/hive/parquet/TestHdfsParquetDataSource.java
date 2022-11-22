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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_FILE_SYSTEM_FACTORY;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsParquetDataSource
{
    @Test(dataProvider = "testPlanReadOrderingProvider")
    public void testPlanReadOrdering(DataSize maxBufferSize)
            throws IOException
    {
        Slice testingInput = Slices.wrappedIntArray(IntStream.range(0, 1000).toArray());
        String path = "/tmp/" + UUID.randomUUID();
        TrinoFileSystem trinoFileSystem = HDFS_FILE_SYSTEM_FACTORY.create(SESSION);
        try (OutputStream outputStream = trinoFileSystem.newOutputFile(path).create(newSimpleAggregatedMemoryContext())) {
            outputStream.write(testingInput.getBytes());
        }
        TrinoParquetDataSource dataSource = new TrinoParquetDataSource(
                trinoFileSystem.newInputFile(path),
                new ParquetReaderOptions().withMaxBufferSize(maxBufferSize),
                new FileFormatDataSourceStats());

        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planRead(ImmutableListMultimap.<String, DiskRange>builder()
                .putAll("test", new DiskRange(0, 300), new DiskRange(400, 100), new DiskRange(700, 200))
                .build());
        assertThat(chunkReaders.get("test"))
                .map(ChunkReader::read)
                .isEqualTo(ImmutableList.of(
                        testingInput.slice(0, 300),
                        testingInput.slice(400, 100),
                        testingInput.slice(700, 200)));
    }

    @DataProvider
    public Object[][] testPlanReadOrderingProvider()
    {
        return new Object[][] {
                {DataSize.ofBytes(0)}, // All large ranges
                {DataSize.ofBytes(200)}, // Mix of large and small ranges
                {DataSize.ofBytes(100000000)}, // All small ranges
        };
    }
}

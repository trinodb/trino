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
package io.trino.parquet.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetReaderOptions;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.stream.IntStream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestParquetDataSource
{
    @Test(dataProvider = "testPlanReadOrderingProvider")
    public void testPlanReadOrdering(DataSize maxBufferSize)
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                new ParquetReaderOptions().withMaxBufferSize(maxBufferSize));

        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planChunksRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .putAll("test", new DiskRange(0, 200), new DiskRange(400, 100), new DiskRange(700, 200))
                        .build(),
                newSimpleAggregatedMemoryContext());
        assertThat(chunkReaders.get("test"))
                .map(ChunkReader::read)
                .isEqualTo(ImmutableList.of(
                        testingInput.slice(0, 200),
                        testingInput.slice(400, 100),
                        testingInput.slice(700, 200)));
    }

    @DataProvider
    public Object[][] testPlanReadOrderingProvider()
    {
        return new Object[][] {
                {DataSize.ofBytes(200)}, // Mix of large and small ranges
                {DataSize.ofBytes(100000000)}, // All small ranges
        };
    }

    @Test
    public void testMemoryAccounting()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                new ParquetReaderOptions().withMaxBufferSize(DataSize.ofBytes(500)));
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planChunksRead(ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("2", new DiskRange(400, 100))
                        .put("3", new DiskRange(700, 200))
                        .build(),
                memoryContext);

        ChunkReader firstReader = Iterables.getOnlyElement(chunkReaders.get("1"));
        ChunkReader secondReader = Iterables.getOnlyElement(chunkReaders.get("2"));
        ChunkReader thirdReader = Iterables.getOnlyElement(chunkReaders.get("3"));
        assertEquals(memoryContext.getBytes(), 0);
        firstReader.read();
        // first and second range are merged
        assertEquals(memoryContext.getBytes(), 500);
        firstReader.free();
        // since the second reader is not freed, the memory is still retained
        assertEquals(memoryContext.getBytes(), 500);
        thirdReader.read();
        // third reader is standalone so only retains its size
        assertEquals(memoryContext.getBytes(), 700);
        thirdReader.free();
        // third reader is standalone, free releases the memory
        assertEquals(memoryContext.getBytes(), 500);
        secondReader.read();
        // second reader is merged with the first, read only accesses already cached data
        assertEquals(memoryContext.getBytes(), 500);
        secondReader.free();
        // both readers using merged reader are freed, all memory is released
        assertEquals(memoryContext.getBytes(), 0);
    }

    @Test
    public void testChunkedInputStreamLazyLoading()
            throws IOException
    {
        Slice testingInput = createTestingInput();
        TestingParquetDataSource dataSource = new TestingParquetDataSource(
                testingInput,
                new ParquetReaderOptions()
                        .withMaxBufferSize(DataSize.ofBytes(500))
                        .withMaxMergeDistance(DataSize.ofBytes(0)));
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();
        Map<String, ChunkedInputStream> inputStreams = dataSource.planRead(
                ImmutableListMultimap.<String, DiskRange>builder()
                        .put("1", new DiskRange(0, 200))
                        .put("1", new DiskRange(250, 50))
                        .put("2", new DiskRange(400, 100))
                        .put("2", new DiskRange(700, 200))
                        .build(),
                memoryContext);
        assertThat(memoryContext.getBytes()).isEqualTo(0);

        inputStreams.get("1").getSlice(200);
        assertThat(memoryContext.getBytes()).isEqualTo(200);

        inputStreams.get("2").getSlice(100);
        assertThat(memoryContext.getBytes()).isEqualTo(200 + 100);

        inputStreams.get("1").close();
        assertThat(memoryContext.getBytes()).isEqualTo(100);
    }

    private static Slice createTestingInput()
    {
        Slice testingInput = Slices.allocate(4000);
        SliceOutput out = testingInput.getOutput();
        IntStream.range(0, 1000).forEach(out::appendInt);
        return testingInput;
    }
}

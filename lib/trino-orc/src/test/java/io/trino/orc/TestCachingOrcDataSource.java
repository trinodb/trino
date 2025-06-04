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
package io.trino.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.stream.OrcDataReader;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static io.trino.orc.OrcRecordReader.wrapWithCacheIfTinyStripes;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnsHiveFile;
import static io.trino.orc.metadata.CompressionKind.ZLIB;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCachingOrcDataSource
{
    private static final int POSITION_COUNT = 50000;

    private final TempFile tempFile;

    public TestCachingOrcDataSource()
            throws Exception
    {
        tempFile = new TempFile();
        Random random = new Random();
        writeOrcColumnsHiveFile(
                tempFile.getFile(),
                ORC_12,
                ZLIB,
                ImmutableList.of("test"),
                ImmutableList.of(VARCHAR),
                Stream.generate(() -> (Function<Integer, Object>) (fieldIndex) -> Long.toHexString(random.nextLong()))
                        .limit(POSITION_COUNT).iterator());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        tempFile.close();
    }

    @Test
    public void testWrapWithCacheIfTinyStripes()
    {
        DataSize maxMergeDistance = DataSize.of(1, Unit.MEGABYTE);
        DataSize tinyStripeThreshold = DataSize.of(8, Unit.MEGABYTE);

        OrcDataSource actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(),
                maxMergeDistance,
                tinyStripeThreshold);
        assertThat(actual).isInstanceOf(CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertThat(actual).isInstanceOf(CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 10, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertThat(actual).isInstanceOf(CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertThat(actual).isInstanceOf(CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20 + 1, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertNotInstanceOf(actual, CachingOrcDataSource.class);
    }

    @Test
    public void testTinyStripesReadCacheAt()
            throws IOException
    {
        DataSize maxMergeDistance = DataSize.of(1, Unit.MEGABYTE);
        DataSize tinyStripeThreshold = DataSize.of(8, Unit.MEGABYTE);

        TestingOrcDataSource testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        CachingOrcDataSource cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        tinyStripeThreshold));
        cachingOrcDataSource.readCacheAt(3);
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        tinyStripeThreshold));
        cachingOrcDataSource.readCacheAt(62); // read at the end of a stripe
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 1, 1, 1), new StripeInformation(123, 4, 1048576, 1048576, 1048576 * 3), new StripeInformation(123, 4 + 1048576 * 5, 1048576, 1048576, 1048576)),
                        maxMergeDistance,
                        tinyStripeThreshold));
        cachingOrcDataSource.readCacheAt(3);
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(3, 1 + 1048576 * 5)));
        cachingOrcDataSource.readCacheAt(4 + 1048576 * 5);
        assertThat(testingOrcDataSource.getLastReadRanges()).isEqualTo(ImmutableList.of(new DiskRange(4 + 1048576 * 5, 3 * 1048576)));
    }

    @Test
    public void testIntegration()
            throws IOException
    {
        // tiny file
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS));
        doIntegration(orcDataSource, DataSize.of(1, Unit.MEGABYTE), DataSize.of(1, Unit.MEGABYTE));
        assertThat(orcDataSource.getReadCount()).isEqualTo(1); // read entire file at once

        // tiny stripes
        orcDataSource = new TestingOrcDataSource(new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS));
        doIntegration(orcDataSource, DataSize.of(400, Unit.KILOBYTE), DataSize.of(400, Unit.KILOBYTE));
        assertThat(orcDataSource.getReadCount()).isEqualTo(3); // footer, first few stripes, last few stripes
    }

    private void doIntegration(TestingOrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize tinyStripeThreshold)
            throws IOException
    {
        OrcReaderOptions options = new OrcReaderOptions()
                .withMaxMergeDistance(maxMergeDistance)
                .withTinyStripeThreshold(tinyStripeThreshold)
                .withMaxReadBlockSize(DataSize.of(1, Unit.MEGABYTE));
        OrcReader orcReader = OrcReader.createOrcReader(orcDataSource, options)
                .orElseThrow(() -> new RuntimeException("File is empty"));
        // 1 for reading file footer
        assertThat(orcDataSource.getReadCount()).isEqualTo(1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        // Sanity check number of stripes. This can be three or higher because of orc writer low memory mode.
        assertThat(stripes).hasSizeGreaterThanOrEqualTo(3);
        //verify wrapped by CachingOrcReader
        assertThat(wrapWithCacheIfTinyStripes(orcDataSource, stripes, maxMergeDistance, tinyStripeThreshold)).isInstanceOf(CachingOrcDataSource.class);

        OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(VARCHAR),
                false,
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new);
        int positionCount = 0;
        while (true) {
            SourcePage page = orcRecordReader.nextPage();
            if (page == null) {
                break;
            }
            Block block = page.getBlock(0);
            positionCount += block.getPositionCount();
        }
        assertThat(positionCount).isEqualTo(POSITION_COUNT);
    }

    private static <T, U extends T> void assertNotInstanceOf(T actual, Class<U> expectedType)
    {
        assertThat(actual)
                .describedAs("actual is null")
                .isNotNull();
        assertThat(expectedType)
                .describedAs("expectedType is null")
                .isNotNull();
        if (expectedType.isInstance(actual)) {
            fail(format("expected:<%s> to not be an instance of <%s>", actual, expectedType.getName()));
        }
    }

    private static class FakeOrcDataSource
            implements OrcDataSource
    {
        public static final FakeOrcDataSource INSTANCE = new FakeOrcDataSource();

        @Override
        public OrcDataSourceId getId()
        {
            return new OrcDataSourceId("fake");
        }

        @Override
        public long getReadBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getReadTimeNanos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getEstimatedSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice readTail(int length)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getRetainedSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Slice readFully(long position, int length)
        {
            return Slices.allocate(length);
        }

        @Override
        public <K> Map<K, OrcDataReader> readFully(Map<K, DiskRange> diskRanges)
        {
            throw new UnsupportedOperationException();
        }
    }
}

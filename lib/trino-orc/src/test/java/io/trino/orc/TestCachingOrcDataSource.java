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
import io.trino.hive.orc.OrcConf;
import io.trino.orc.OrcTester.Format;
import io.trino.orc.metadata.CompressionKind;
import io.trino.orc.metadata.StripeInformation;
import io.trino.orc.stream.OrcDataReader;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.trino.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static io.trino.orc.OrcRecordReader.wrapWithCacheIfTinyStripes;
import static io.trino.orc.OrcTester.Format.ORC_12;
import static io.trino.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static io.trino.orc.OrcTester.READER_OPTIONS;
import static io.trino.orc.OrcTester.writeOrcColumnsHiveFile;
import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.orc.metadata.CompressionKind.ZLIB;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestCachingOrcDataSource
{
    private static final int POSITION_COUNT = 50000;

    private TempFile tempFile;

    @BeforeClass
    public void setUp()
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

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        tempFile.close();
        tempFile = null;
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
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 10, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                maxMergeDistance,
                tinyStripeThreshold);
        assertInstanceOf(actual, CachingOrcDataSource.class);

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
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        tinyStripeThreshold));
        cachingOrcDataSource.readCacheAt(62); // read at the end of a stripe
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 1, 1, 1), new StripeInformation(123, 4, 1048576, 1048576, 1048576 * 3), new StripeInformation(123, 4 + 1048576 * 5, 1048576, 1048576, 1048576)),
                        maxMergeDistance,
                        tinyStripeThreshold));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 1 + 1048576 * 5)));
        cachingOrcDataSource.readCacheAt(4 + 1048576 * 5);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(4 + 1048576 * 5, 3 * 1048576)));
    }

    @Test
    public void testIntegration()
            throws IOException
    {
        // tiny file
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS));
        doIntegration(orcDataSource, DataSize.of(1, Unit.MEGABYTE), DataSize.of(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1); // read entire file at once

        // tiny stripes
        orcDataSource = new TestingOrcDataSource(new FileOrcDataSource(tempFile.getFile(), READER_OPTIONS));
        doIntegration(orcDataSource, DataSize.of(400, Unit.KILOBYTE), DataSize.of(400, Unit.KILOBYTE));
        assertEquals(orcDataSource.getReadCount(), 3); // footer, first few stripes, last few stripes
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
        assertEquals(orcDataSource.getReadCount(), 1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        // Sanity check number of stripes. This can be three or higher because of orc writer low memory mode.
        assertGreaterThanOrEqual(stripes.size(), 3);
        //verify wrapped by CachingOrcReader
        assertInstanceOf(wrapWithCacheIfTinyStripes(orcDataSource, stripes, maxMergeDistance, tinyStripeThreshold), CachingOrcDataSource.class);

        OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(VARCHAR),
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new);
        int positionCount = 0;
        while (true) {
            Page page = orcRecordReader.nextPage();
            if (page == null) {
                break;
            }
            page = page.getLoadedPage();
            Block block = page.getBlock(0);
            positionCount += block.getPositionCount();
        }
        assertEquals(positionCount, POSITION_COUNT);
    }

    private static <T, U extends T> void assertNotInstanceOf(T actual, Class<U> expectedType)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expectedType, "expectedType is null");
        if (expectedType.isInstance(actual)) {
            fail(format("expected:<%s> to not be an instance of <%s>", actual, expectedType.getName()));
        }
    }

    private static FileSinkOperator.RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf(newEmptyConfiguration());
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());

        Properties tableProperties = new Properties();
        tableProperties.setProperty(IOConstants.COLUMNS, "test");
        tableProperties.setProperty(IOConstants.COLUMNS_TYPES, columnObjectInspector.getTypeName());
        tableProperties.setProperty(OrcConf.STRIPE_SIZE.getAttribute(), "120000");

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                tableProperties,
                () -> {});
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

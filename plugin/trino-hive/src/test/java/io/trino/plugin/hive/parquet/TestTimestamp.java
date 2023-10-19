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

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.plugin.hive.benchmark.StandardFileFormats;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.schema.MessageType;
import org.joda.time.DateTimeZone;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.cycle;
import static com.google.common.collect.Iterables.limit;
import static com.google.common.collect.Iterables.transform;
import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.NANOSECONDS_PER_MICROSECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_NANOSECOND;
import static io.trino.testing.DataProviders.toDataProvider;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetOutputFormat.WRITER_VERSION;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTimestamp
{
    @DataProvider
    public static Object[][] timestampPrecisionProvider()
    {
        return Arrays.stream(HiveTimestampPrecision.values()).collect(toDataProvider());
    }

    @Test(dataProvider = "timestampPrecisionProvider")
    public void testTimestampBackedByInt64(HiveTimestampPrecision timestamp)
            throws Exception
    {
        String logicalAnnotation = switch (timestamp) {
            case MILLISECONDS -> "TIMESTAMP(MILLIS,true)";
            case MICROSECONDS -> "TIMESTAMP(MICROS,true)";
            case NANOSECONDS -> "TIMESTAMP(NANOS,true)";
        };
        MessageType parquetSchema = parseMessageType("message hive_timestamp { optional int64 test (" + logicalAnnotation + "); }");

        Iterable<Long> writeNullableDictionaryValues = limit(cycle(asList(1L, null, 3L, 5L, null, null, null, 7L, 11L, null, 13L, 17L)), 30_000);
        testRoundTrip(parquetSchema, writeNullableDictionaryValues, timestamp);

        Iterable<Long> writeDictionaryValues = limit(cycle(asList(1L, 3L, 5L, 7L, 11L, 13L, 17L)), 30_000);
        testRoundTrip(parquetSchema, writeDictionaryValues, timestamp);

        Iterable<Long> writeValues = ContiguousSet.create(Range.closedOpen((long) -1_000, (long) 1_000), DiscreteDomain.longs());
        testRoundTrip(parquetSchema, writeValues, timestamp);
    }

    private static void testRoundTrip(MessageType parquetSchema, Iterable<Long> writeValues, HiveTimestampPrecision timestamp)
            throws Exception
    {
        Iterable<SqlTimestamp> timestampReadValues = transform(writeValues, value -> {
            if (value == null) {
                return null;
            }
            return switch (timestamp) {
                case MILLISECONDS -> SqlTimestamp.fromMillis(timestamp.getPrecision(), value);
                case MICROSECONDS -> SqlTimestamp.newInstance(timestamp.getPrecision(), value, 0);
                case NANOSECONDS -> SqlTimestamp.newInstance(
                        timestamp.getPrecision(),
                        floorDiv(value, NANOSECONDS_PER_MICROSECOND),
                        floorMod(value, NANOSECONDS_PER_MICROSECOND) * PICOSECONDS_PER_NANOSECOND);
            };
        });

        List<ObjectInspector> objectInspectors = singletonList(javaLongObjectInspector);
        List<String> columnNames = ImmutableList.of("test");

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf(newEmptyConfiguration());
            jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);

            ParquetTester.writeParquetColumn(
                    jobConf,
                    tempFile.getFile(),
                    CompressionCodec.SNAPPY,
                    ParquetTester.createTableProperties(columnNames, objectInspectors),
                    getStandardStructObjectInspector(columnNames, objectInspectors),
                    new Iterator<?>[] {writeValues.iterator()},
                    Optional.of(parquetSchema),
                    false,
                    DateTimeZone.getDefault());

            ConnectorSession session = getHiveSession(new HiveConfig());
            testReadingAs(createTimestampType(timestamp.getPrecision()), session, tempFile, columnNames, timestampReadValues);
            testReadingAs(BIGINT, session, tempFile, columnNames, writeValues);
        }
    }

    private static void testReadingAs(Type type, ConnectorSession session, ParquetTester.TempFile tempFile, List<String> columnNames, Iterable<?> expectedValues)
             throws IOException
    {
        Iterator<?> expected = expectedValues.iterator();
        try (ConnectorPageSource pageSource = StandardFileFormats.TRINO_PARQUET.createFileFormatReader(session, HDFS_ENVIRONMENT, tempFile.getFile(), columnNames, ImmutableList.of(type))) {
            // skip a page to exercise the decoder's skip() logic
            Page firstPage = pageSource.getNextPage();
            assertTrue(firstPage.getPositionCount() > 0, "Expected first page to have at least 1 row");

            for (int i = 0; i < firstPage.getPositionCount(); i++) {
                expected.next();
            }

            int pageCount = 1;
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page == null) {
                    continue;
                }
                pageCount++;
                Block block = page.getBlock(0);

                for (int i = 0; i < block.getPositionCount(); i++) {
                    assertThat(type.getObjectValue(session, block, i)).isEqualTo(expected.next());
                }
            }

            assertThat(pageCount)
                    .withFailMessage("Expected more than one page but processed %s", pageCount)
                    .isGreaterThan(1);

            assertFalse(expected.hasNext(), "Read fewer values than expected");
        }
    }
}

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
import io.trino.plugin.hive.benchmark.StandardFileFormats;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.SqlTimestamp;
import io.trino.spi.type.Type;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static java.lang.String.format;
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
    @Test
    public void testAnnotatedInt64Timestamp()
            throws Exception
    {
        testInt64TimestampColumn("optional int64 test (TIMESTAMP_MILLIS)");
    }

    @Test
    public void testRawInt64Timestamp()
            throws Exception
    {
        testInt64TimestampColumn("optional int64 test");
    }

    void testInt64TimestampColumn(String column)
            throws Exception
    {
        MessageType parquetSchema = parseMessageType(format("message hive_timestamp { %s; }", column));
        ContiguousSet<Long> epochMillisValues = ContiguousSet.create(Range.closedOpen((long) -1_000, (long) 1_000), DiscreteDomain.longs());
        ImmutableList.Builder<SqlTimestamp> timestampsMillis = new ImmutableList.Builder<>();
        ImmutableList.Builder<Long> bigints = new ImmutableList.Builder<>();
        for (long value : epochMillisValues) {
            timestampsMillis.add(SqlTimestamp.fromMillis(3, value));
            bigints.add(value);
        }

        List<ObjectInspector> objectInspectors = singletonList(javaLongObjectInspector);
        List<String> columnNames = ImmutableList.of("test");

        ConnectorSession session = getHiveSession(new HiveConfig());

        try (ParquetTester.TempFile tempFile = new ParquetTester.TempFile("test", "parquet")) {
            JobConf jobConf = new JobConf();
            jobConf.setEnum(WRITER_VERSION, PARQUET_1_0);

            ParquetTester.writeParquetColumn(
                    jobConf,
                    tempFile.getFile(),
                    CompressionCodecName.SNAPPY,
                    ParquetTester.createTableProperties(columnNames, objectInspectors),
                    getStandardStructObjectInspector(columnNames, objectInspectors),
                    new Iterator<?>[] {epochMillisValues.iterator()},
                    Optional.of(parquetSchema),
                    false);

            testReadingAs(TIMESTAMP_MILLIS, session, tempFile, columnNames, timestampsMillis.build());
            testReadingAs(BIGINT, session, tempFile, columnNames, bigints.build());
        }
    }

    private void testReadingAs(Type type, ConnectorSession session, ParquetTester.TempFile tempFile, List<String> columnNames, List<?> expectedValues)
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

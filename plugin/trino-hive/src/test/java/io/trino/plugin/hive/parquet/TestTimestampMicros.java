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
import com.google.common.io.Resources;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.benchmark.StandardFileFormats.TRINO_PARQUET;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampMicros
{
    @Test(dataProvider = "testTimestampMicrosDataProvider")
    public void testTimestampMicros(HiveTimestampPrecision timestampPrecision, LocalDateTime expected, boolean useOptimizedParquetReader)
            throws Exception
    {
        ConnectorSession session = getHiveSession(
                new HiveConfig().setTimestampPrecision(timestampPrecision),
                new ParquetReaderConfig().setOptimizedReaderEnabled(useOptimizedParquetReader));

        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, "created", columnType)) {
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected)));
        }
    }

    @Test(dataProvider = "testTimestampMicrosDataProvider")
    public void testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision timestampPrecision, LocalDateTime expected, boolean useOptimizedParquetReader)
            throws Exception
    {
        ConnectorSession session = getHiveSession(
                new HiveConfig().setTimestampPrecision(timestampPrecision),
                new ParquetReaderConfig().setOptimizedReaderEnabled(useOptimizedParquetReader));

        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampWithTimeZoneType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, "created", columnType)) {
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected.atZone(ZoneId.of("UTC")))));
        }
    }

    @DataProvider
    public static Object[][] testTimestampMicrosDataProvider()
    {
        return cartesianProduct(
                new Object[][] {
                        {HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907")},
                        {HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668")},
                        {HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668")},
                },
                new Object[][] {{true}, {false}});
    }

    private ConnectorPageSource createPageSource(ConnectorSession session, File parquetFile, String columnName, Type columnType)
    {
        return TRINO_PARQUET.createFileFormatReader(session, HDFS_ENVIRONMENT, parquetFile, ImmutableList.of(columnName), ImmutableList.of(columnType));
    }
}

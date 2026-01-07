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

import com.google.common.io.Resources;
import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

import static io.trino.metastore.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.parquet.ParquetUtil.createPageSource;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampPrecision
{
    @Test
    public void testTimestampMicros()
            throws Exception
    {
        // issue-5483.parquet contains a timestamp with microsecond precision
        testTimestamp("issue-5483.parquet", HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestamp("issue-5483.parquet", HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestamp("issue-5483.parquet", HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
    }

    @Test
    public void testTimestampNanos()
            throws Exception
    {
        // timestamp-nanos.parquet contains a timestamp with nanosecond precision
        testTimestamp("timestamp-nanos.parquet", HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestamp("timestamp-nanos.parquet", HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestamp("timestamp-nanos.parquet", HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668123"));
    }

    private void testTimestamp(String filename, HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        File parquetFile = new File(Resources.getResource(filename).toURI());
        Type columnType = createTimestampType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(SESSION, parquetFile, List.of(createBaseColumn("created", 0, HIVE_TIMESTAMP, columnType, REGULAR, Optional.empty())), TupleDomain.all())) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected)));
        }
    }

    @Test
    public void testTimestampMicrosAsTimestampWithTimeZone()
            throws Exception
    {
        // issue-5483.parquet contains a timestamp with microsecond precision
        testTimestampAsTimestampWithTimeZone("issue-5483.parquet", HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestampAsTimestampWithTimeZone("issue-5483.parquet", HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestampAsTimestampWithTimeZone("issue-5483.parquet", HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
    }

    @Test
    public void testTimestampNanosAsTimestampWithTimeZone()
            throws Exception
    {
        // timestamp-nanos.parquet contains a timestamp with nanosecond precision
        testTimestampAsTimestampWithTimeZone("timestamp-nanos.parquet", HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestampAsTimestampWithTimeZone("timestamp-nanos.parquet", HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestampAsTimestampWithTimeZone("timestamp-nanos.parquet", HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668123"));
    }

    private void testTimestampAsTimestampWithTimeZone(String filename, HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        File parquetFile = new File(Resources.getResource(filename).toURI());
        Type columnType = createTimestampWithTimeZoneType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(SESSION, parquetFile, List.of(createBaseColumn("created", 0, HIVE_TIMESTAMP, columnType, REGULAR, Optional.empty())), TupleDomain.all())) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected.atZone(ZoneId.of("UTC")))));
        }
    }
}

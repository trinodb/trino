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

import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.SESSION;
import static io.trino.plugin.hive.HiveType.HIVE_TIMESTAMP;
import static io.trino.plugin.hive.parquet.ParquetUtil.createPageSource;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTimestampMicros
{
    @Test
    public void testTimestampMicros()
            throws Exception
    {
        testTimestampMicros(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestampMicros(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestampMicros(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
    }

    private void testTimestampMicros(HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
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
        testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision.MILLISECONDS, LocalDateTime.parse("2020-10-12T16:26:02.907"));
        testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision.MICROSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
        testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision.NANOSECONDS, LocalDateTime.parse("2020-10-12T16:26:02.906668"));
    }

    private void testTimestampMicrosAsTimestampWithTimeZone(HiveTimestampPrecision timestampPrecision, LocalDateTime expected)
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("issue-5483.parquet").toURI());
        Type columnType = createTimestampWithTimeZoneType(timestampPrecision.getPrecision());

        try (ConnectorPageSource pageSource = createPageSource(SESSION, parquetFile, List.of(createBaseColumn("created", 0, HIVE_TIMESTAMP, columnType, REGULAR, Optional.empty())), TupleDomain.all())) {
            MaterializedResult result = materializeSourceDataStream(SESSION, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getMaterializedRows())
                    .containsOnly(new MaterializedRow(List.of(expected.atZone(ZoneId.of("UTC")))));
        }
    }
}

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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;

public class TestDeltaLakeCreateTableStatistics
        extends AbstractTestDeltaLakeCreateTableStatistics
{
    @Override
    Map<String, String> additionalProperties()
    {
        return ImmutableMap.of();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        verify(!new ParquetWriterConfig().isParquetOptimizedWriterEnabled(), "This test assumes the optimized Parquet writer is disabled by default");
        return super.createQueryRunner();
    }

    @Override
    @Test
    public void testTimestampMilliRecords()
            throws IOException
    {
        String columnName = "t_timestamp";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, TIMESTAMP_TZ_MILLIS, REGULAR);
        try (TestTable table = new TestTable(
                "test_timestamp_records_",
                ImmutableList.of(columnName),
                "VALUES timestamp '2012-10-31 01:00:00.123 America/New_York', timestamp '2012-10-31 01:00:00.123 America/Los_Angeles', timestamp '2012-10-31 01:00:00.123 UTC'")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.empty());
            assertEquals(fileStatistics.getNullCount(columnName), Optional.empty());
        }
    }

    // int96 timestamp Statistics are only populated if a row group contains a single value
    @Test
    public void testTimestampMilliSingleRecord()
            throws IOException
    {
        String columnName = "t_timestamp";
        DeltaLakeColumnHandle columnHandle = new DeltaLakeColumnHandle(columnName, TIMESTAMP_TZ_MILLIS, REGULAR);
        try (TestTable table = new TestTable(
                "test_timestamp_single_record_",
                ImmutableList.of(columnName),
                "VALUES timestamp '2012-10-31 04:00:00.123 America/New_York', timestamp '2012-10-31 01:00:00.123 America/Los_Angeles', null")) {
            List<AddFileEntry> addFileEntries = getAddFileEntries(table.getName());
            AddFileEntry entry = getOnlyElement(addFileEntries);
            assertThat(entry.getStats()).isPresent();
            DeltaLakeFileStatistics fileStatistics = entry.getStats().get();

            assertEquals(fileStatistics.getNumRecords(), Optional.of(3L));
            Function<String, Long> timestampValueConverter = valueString -> {
                ZonedDateTime zonedDateTime = ZonedDateTime.parse(valueString);
                Instant instant = zonedDateTime.toInstant();
                return packDateTimeWithZone(instant.toEpochMilli(), UTC_KEY);
            };
            assertEquals(fileStatistics.getMinColumnValue(columnHandle), Optional.of(timestampValueConverter.apply("2012-10-31T08:00:00.123Z")));
            assertEquals(fileStatistics.getMaxColumnValue(columnHandle), Optional.of(timestampValueConverter.apply("2012-10-31T08:00:00.123Z")));
            assertEquals(fileStatistics.getNullCount(columnName), Optional.of(1L));
        }
    }
}

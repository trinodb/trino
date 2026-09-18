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
package io.trino.plugin.deltalake.transactionlog;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.OptionalInt;

import static org.assertj.core.api.Assertions.assertThat;

final class TestAddFileEntry
{
    private static final DeltaLakeParquetFileStatistics PARQUET_STATISTICS = new DeltaLakeParquetFileStatistics(
            Optional.of(5L),
            Optional.of(ImmutableMap.of("x", 1L)),
            Optional.of(ImmutableMap.of("x", 5L)),
            Optional.of(ImmutableMap.of("x", 0L)));

    @Test
    void testParquetStatisticsWithJsonBounds()
    {
        AddFileEntry entry = createEntry(Optional.of("{\"numRecords\":4,\"minValues\":{\"x\":2},\"tightBounds\":false}"));
        assertThat(entry.getStats().orElseThrow()).isEqualTo(new DeltaLakeParquetFileStatistics(
                PARQUET_STATISTICS.getNumRecords(),
                PARQUET_STATISTICS.getMinValues(),
                PARQUET_STATISTICS.getMaxValues(),
                PARQUET_STATISTICS.getNullCount(),
                Optional.of(false)));
    }

    @Test
    void testParquetStatisticsWithMalformedJson()
    {
        AddFileEntry entry = createEntry(Optional.of("{\"tightBounds\":false,"));
        assertThat(entry.getStats().orElseThrow()).isSameAs(PARQUET_STATISTICS);
    }

    @Test
    void testParquetStatisticsWithoutJsonBounds()
    {
        assertThat(createEntry(Optional.of("{\"numRecords\":5}")).getStats().orElseThrow()).isSameAs(PARQUET_STATISTICS);
        assertThat(createEntry(Optional.empty()).getStats().orElseThrow()).isSameAs(PARQUET_STATISTICS);
    }

    private static AddFileEntry createEntry(Optional<String> jsonStatistics)
    {
        return new AddFileEntry(
                "file.parquet",
                ImmutableMap.of(),
                100,
                1000,
                false,
                jsonStatistics,
                Optional.of(PARQUET_STATISTICS),
                ImmutableMap.of(),
                Optional.of(new DeletionVectorEntry("i", "encoded", OptionalInt.empty(), 1, 1)));
    }
}

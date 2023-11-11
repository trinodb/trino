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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveType;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSessionProperties;
import static io.trino.plugin.hive.parquet.ParquetUtil.createPageSource;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetFilteredRowRanges
{
    @Test
    public void testFilteredRowRanges()
            throws Exception
    {
        // The file contains lineitem table with column index sorted by suppkey
        File parquetFile = new File(Resources.getResource(
                        "parquet_page_skipping/lineitem_sorted_by_suppkey/data.parquet")
                .toURI());
        String columnName = "suppkey";
        Type columnType = BIGINT;

        HiveColumnHandle column = createBaseColumn(columnName, 0, HiveType.toHiveType(columnType), columnType, REGULAR, Optional.empty());
        ConnectorSession session = getHiveSession(new HiveConfig(), new ParquetReaderConfig().setUseColumnIndex(true));

        TupleDomain<HiveColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(columnType, 1000L)));
        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, List.of(column), domain)) {
            verifyRowRanges(0, pageSource.getNextFilteredRowRanges().orElseThrow());
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getRowCount()).isEqualTo(0);
        }

        domain = TupleDomain.withColumnDomains(Map.of(column, Domain.multipleValues(columnType, ImmutableList.of(10L, 40L, 60L))));
        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, List.of(column), domain)) {
            verifyRowRanges(
                    11282,
                    pageSource.getNextFilteredRowRanges().orElseThrow(),
                    3752, 7593,
                    22524, 26255,
                    33792, 37502);
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getRowCount()).isEqualTo(11282);
        }

        session = getHiveSession(new HiveConfig(), new ParquetReaderConfig().setUseColumnIndex(false));
        try (ConnectorPageSource pageSource = createPageSource(session, parquetFile, List.of(column), domain)) {
            verifyRowRanges(60175, pageSource.getNextFilteredRowRanges().orElseThrow(), 0, 60175);
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getRowCount()).isEqualTo(60175);
        }
    }

    private static TestingConnectorSession getHiveSession(HiveConfig hiveConfig, ParquetReaderConfig parquetReaderConfig)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(getHiveSessionProperties(hiveConfig, parquetReaderConfig).getSessionProperties())
                .build();
    }

    private static void verifyRowRanges(long expectedRowCount, ConnectorPageSource.RowRanges rowRanges, long... expectedValues)
    {
        checkArgument(expectedValues.length % 2 == 0);
        assertThat(rowRanges.getRowCount()).isEqualTo(expectedRowCount);
        assertThat(rowRanges.getRangesCount()).isEqualTo(expectedValues.length / 2);
        assertThat(rowRanges.isNoMoreRowRanges()).isTrue();
        for (int rangeIndex = 0; rangeIndex < rowRanges.getRangesCount(); rangeIndex++) {
            assertThat(rowRanges.getLowerInclusive(rangeIndex)).isEqualTo(expectedValues[2 * rangeIndex]);
            assertThat(rowRanges.getUpperExclusive(rangeIndex)).isEqualTo(expectedValues[(2 * rangeIndex) + 1]);
        }
    }
}

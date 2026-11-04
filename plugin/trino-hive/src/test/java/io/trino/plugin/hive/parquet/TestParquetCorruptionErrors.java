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
import io.airlift.units.DataSize;
import io.trino.filesystem.local.LocalInputFile;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.trino.plugin.hive.parquet.ParquetPageSourceFactory.createPageSource;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.joda.time.DateTimeZone.UTC;

public class TestParquetCorruptionErrors
{
    @Test
    public void testOversizedPageIsReportedAsBadData()
            throws Exception
    {
        File parquetFile = new File(Resources.getResource("issue-10873.parquet").toURI());
        HiveColumnHandle column = createBaseColumn("x", 0, HIVE_INT, INTEGER, REGULAR, Optional.empty());
        ParquetReaderOptions options = ParquetReaderOptions.builder()
                .withMaxPageReadSize(DataSize.ofBytes(1))
                .build();

        try (ConnectorPageSource pageSource = createPageSource(
                new LocalInputFile(parquetFile),
                0,
                parquetFile.length(),
                List.of(column),
                List.of(TupleDomain.all()),
                true,
                UTC,
                new FileFormatDataSourceStats(),
                options,
                Optional.empty(),
                Optional.empty(),
                100,
                OptionalLong.of(parquetFile.length()),
                MemoryContext.NO_LIMIT)) {
            SourcePage page = pageSource.getNextSourcePage();
            assertThat(page).isNotNull();
            assertThatThrownBy(() -> page.getBlock(0))
                    .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(HIVE_BAD_DATA.toErrorCode()))
                    .hasMessageContaining("exceeds maximum allowed size")
                    .hasMessageContaining("issue-10873.parquet")
                    .hasMessageContaining("[x]");
        }
    }
}

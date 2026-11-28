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
package io.trino.plugin.redshift;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.filesystem.Location;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.JdbcTransactionHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.parquet.ParquetTestUtils.writeParquetFile;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

final class TestRedshiftPageSourceProvider
{
    @Test
    void testUnloadPageSourceReportsMemoryToMemoryContext()
            throws Exception
    {
        int rowCount = 100_000;
        Slice parquetFile = writeParquetFile(
                ParquetWriterOptions.builder().build(),
                ImmutableList.of(BIGINT),
                ImmutableList.of("value"),
                ImmutableList.of(new Page(createLongSequenceBlock(0, rowCount))));
        MemoryFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        Location location = Location.of("memory:///unload/part-0.parquet");
        fileSystemFactory.create(SESSION).newOutputFile(location).createOrOverwrite(parquetFile.getBytes());

        RedshiftPageSourceProvider pageSourceProvider = new RedshiftPageSourceProvider(
                new ConnectorPageSourceProvider() {},
                fileSystemFactory,
                new FileFormatDataSourceStats());
        ColumnHandle valueColumn = new JdbcColumnHandle("value", JDBC_BIGINT, BIGINT);
        JdbcTableHandle table = new JdbcTableHandle(
                new SchemaTableName("schema", "table"),
                new RemoteTableName(Optional.empty(), Optional.of("schema"), "table"),
                Optional.empty());

        AtomicLong reportedBytes = new AtomicLong();
        long maxReportedBytes = 0;
        long positions = 0;
        try (ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new JdbcTransactionHandle(),
                SESSION,
                new RedshiftUnloadSplit(location.toString(), parquetFile.length()),
                table,
                Optional.empty(),
                ImmutableList.of(valueColumn),
                DynamicFilter.EMPTY,
                reportedBytes::set)) {
            assertThat(pageSource).isInstanceOf(RedshiftParquetPageSource.class);
            while (!pageSource.isFinished()) {
                SourcePage page = pageSource.getNextSourcePage();
                if (page != null) {
                    // load the lazy block so that the column chunk is read
                    assertThat(page.getBlock(0).getPositionCount()).isEqualTo(page.getPositionCount());
                    positions += page.getPositionCount();
                    maxReportedBytes = Math.max(maxReportedBytes, reportedBytes.get());
                    // the reader reports to the memory context, so polling must not account for it again
                    assertThat(pageSource.getMemoryUsage()).isZero();
                }
            }
        }
        assertThat(positions).isEqualTo(rowCount);
        assertThat(maxReportedBytes).isPositive();
        assertThat(reportedBytes.get()).isZero();
    }
}

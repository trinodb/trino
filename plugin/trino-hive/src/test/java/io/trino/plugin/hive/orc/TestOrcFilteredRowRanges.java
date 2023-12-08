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
package io.trino.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.orc.OrcReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePageSourceProvider;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.util.SerdeConstants;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.io.Resources.getResource;
import static io.trino.hive.thrift.metastore.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HivePageSourceProvider.ColumnMapping.buildColumnMappings;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveTestUtils.getHiveSession;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;

public class TestOrcFilteredRowRanges
{
    @Test
    public void testFilteredRowRanges()
            throws Exception
    {
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        // The file contains lineitem table with column index sorted by suppkey
        Location orcFile = copyResource(fileSystemFactory, "lineitem_sorted_by_suppkey.orc");
        String columnName = "suppkey";
        Type columnType = BIGINT;

        HiveColumnHandle column = createBaseColumn(columnName, 0, HiveType.toHiveType(columnType), columnType, REGULAR, Optional.empty());
        ConnectorSession session = getHiveSession(new HiveConfig());

        TupleDomain<HiveColumnHandle> domain = TupleDomain.withColumnDomains(Map.of(column, Domain.singleValue(columnType, 1000L)));
        try (ConnectorPageSource pageSource = createPageSource(session, fileSystemFactory, orcFile, List.of(column), domain)) {
            verifyRowRanges(0, pageSource.getNextFilteredRowRanges().orElseThrow());
            MaterializedResult result = materializeSourceDataStream(session, pageSource, List.of(columnType)).toTestTypes();
            assertThat(result.getRowCount()).isEqualTo(0);
        }

        domain = TupleDomain.withColumnDomains(Map.of(column, Domain.multipleValues(columnType, ImmutableList.of(10L, 15L, 40L, 75L))));
        try (ConnectorPageSource pageSource = createPageSource(session, fileSystemFactory, orcFile, List.of(column), domain)) {
            long rowCountProcessed = pageSource.getNextPage().getPositionCount();
            long rangesRowCount = 0;
            ConnectorPageSource.RowRanges rowRanges = pageSource.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
            verifyRowRanges(
                    2000,
                    rowRanges,
                    5000, 6000,
                    8000, 9000);
            rangesRowCount += rowRanges.getRowCount();

            while (rowCountProcessed <= rangesRowCount) {
                rowCountProcessed += pageSource.getNextPage().getPositionCount();
            }

            rowRanges = pageSource.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isFalse();
            verifyRowRanges(
                    2000,
                    rowRanges,
                    23000, 25000);
            rangesRowCount += rowRanges.getRowCount();

            while (rowCountProcessed <= rangesRowCount) {
                rowCountProcessed += pageSource.getNextPage().getPositionCount();
            }

            rowRanges = pageSource.getNextFilteredRowRanges().orElseThrow();
            assertThat(rowRanges.isNoMoreRowRanges()).isTrue();
            verifyRowRanges(
                    2000,
                    rowRanges,
                    44000, 46000);
            rangesRowCount += rowRanges.getRowCount();

            while (rowCountProcessed < rangesRowCount) {
                rowCountProcessed += pageSource.getNextPage().getPositionCount();
            }

            assertThat(pageSource.getNextPage()).isNull();
            assertThat(rowCountProcessed).isEqualTo(rangesRowCount);
            assertThat(rowCountProcessed).isEqualTo(6000);
        }
    }

    private static ConnectorPageSource createPageSource(
            ConnectorSession session,
            TrinoFileSystemFactory fileSystemFactory,
            Location location,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate)
            throws IOException
    {
        OrcPageSourceFactory readerFactory = new OrcPageSourceFactory(new OrcReaderOptions(), fileSystemFactory, new FileFormatDataSourceStats(), UTC);

        long length = fileSystemFactory.create(session).newInputFile(location).length();
        List<HivePageSourceProvider.ColumnMapping> columnMappings = buildColumnMappings(
                "",
                ImmutableList.of(),
                columns,
                ImmutableList.of(),
                ImmutableMap.of(),
                location.toString(),
                OptionalInt.empty(),
                length,
                Instant.now().toEpochMilli());

        return HivePageSourceProvider.createHivePageSource(
                        ImmutableSet.of(readerFactory),
                        session,
                        location,
                        OptionalInt.empty(),
                        0,
                        length,
                        length,
                        getTableProperties(columns),
                        effectivePredicate,
                        TESTING_TYPE_MANAGER,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        NO_ACID_TRANSACTION,
                        columnMappings)
                .orElseThrow();
    }

    private static Map<String, String> getTableProperties(List<HiveColumnHandle> columns)
    {
        return ImmutableMap.<String, String>builder()
                .put(FILE_INPUT_FORMAT, ORC.getInputFormat())
                .put(SerdeConstants.SERIALIZATION_LIB, ORC.getSerde())
                .put(LIST_COLUMNS, columns.stream().map(HiveColumnHandle::getName).collect(Collectors.joining(",")))
                .put(LIST_COLUMN_TYPES, columns.stream().map(HiveColumnHandle::getHiveType).map(HiveType::toString).collect(Collectors.joining(",")))
                .buildOrThrow();
    }

    private static Location copyResource(TrinoFileSystemFactory fileSystemFactory, String resourceName)
            throws IOException
    {
        Location location = Location.of("memory:///" + resourceName);
        TrinoFileSystem fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser("test"));
        try (OutputStream outputStream = fileSystem.newOutputFile(location).create()) {
            Resources.copy(getResource(resourceName), outputStream);
        }
        return location;
    }

    private static void verifyRowRanges(long expectedRowCount, ConnectorPageSource.RowRanges rowRanges, long... expectedValues)
    {
        checkArgument(expectedValues.length % 2 == 0);
        assertThat(rowRanges.getRangesCount()).isEqualTo(expectedValues.length / 2);
        assertThat(rowRanges.getRowCount()).isEqualTo(expectedRowCount);
        for (int rangeIndex = 0; rangeIndex < rowRanges.getRangesCount(); rangeIndex++) {
            assertThat(rowRanges.getLowerInclusive(rangeIndex)).isEqualTo(expectedValues[2 * rangeIndex]);
            assertThat(rowRanges.getUpperExclusive(rangeIndex)).isEqualTo(expectedValues[(2 * rangeIndex) + 1]);
        }
    }
}

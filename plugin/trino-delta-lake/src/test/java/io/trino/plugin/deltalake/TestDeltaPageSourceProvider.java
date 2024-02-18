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
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDeltaPageSourceProvider
{
    private DeltaLakePageSourceProvider pageSourceProvider;

    @BeforeAll
    public void setUp()
            throws IOException
    {
        pageSourceProvider = new DeltaLakePageSourceProvider(
                new LocalFileSystemFactory(Files.createTempDirectory("prefix")),
                new FileFormatDataSourceStats(),
                new ParquetReaderConfig(),
                new DeltaLakeConfig(),
                TESTING_TYPE_MANAGER);
    }

    @Test
    public void testPrunePredicate()
    {
        String partitionedColumn = "partitionedColumn";
        ColumnHandle partitionedColumnHandle = prepareColumnHandle(partitionedColumn, BIGINT, DeltaLakeColumnType.PARTITION_KEY);
        ColumnHandle regularColumnHandle = prepareColumnHandle("regular", BIGINT, DeltaLakeColumnType.REGULAR);
        MetadataEntry metadataEntry = createMetadataEntry(
                ImmutableList.of(partitionedColumn),
                """
                        {"fields":[{"name":"partitionedColumn","type":"long","nullable":false,"metadata":{}}]}"
                        """);
        DeltaLakeTableHandle tableHandle = createDeltaLakeTableHandle(metadataEntry, TupleDomain.all());
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                partitionedColumnHandle, Domain.singleValue(BIGINT, 0L),
                regularColumnHandle, Domain.singleValue(BIGINT, 0L)
        ));
        TupleDomain<ColumnHandle> prunedPredicate = pageSourceProvider.prunePredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.all(), ImmutableMap.of("partitionedColumn", Optional.of("0"))),
                tableHandle,
                predicate
        );
        assertThat(prunedPredicate).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.singleValue(BIGINT, 0L))));
    }

    @Test
    public void testPrunePredicateWhenSplitIsFilteredOut()
    {
        String partitionedColumn = "partitionedColumn";
        ColumnHandle partitionedColumnHandle = prepareColumnHandle(partitionedColumn, BIGINT, DeltaLakeColumnType.PARTITION_KEY);
        ColumnHandle regularColumnHandle = prepareColumnHandle("regular", BIGINT, DeltaLakeColumnType.REGULAR);
        MetadataEntry metadataEntry = createMetadataEntry(
                ImmutableList.of(partitionedColumn),
                """
                        {"fields":[{"name":"partitionedColumn","type":"long","nullable":false,"metadata":{}}]}"
                        """);
        DeltaLakeTableHandle tableHandle = createDeltaLakeTableHandle(metadataEntry, TupleDomain.all());
        TupleDomain<ColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                partitionedColumnHandle, Domain.singleValue(BIGINT, 0L),
                regularColumnHandle, Domain.singleValue(BIGINT, 0L)
        ));
        TupleDomain<ColumnHandle> prunedPredicate = pageSourceProvider.prunePredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.all(), ImmutableMap.of("partitionedColumn", Optional.of("1"))),
                tableHandle,
                predicate
        );
        assertThat(prunedPredicate).isEqualTo(TupleDomain.none());
        prunedPredicate = pageSourceProvider.prunePredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.all(), ImmutableMap.of("partitionedColumn", Optional.of("0"))),
                tableHandle,
                predicate
        );
        assertThat(prunedPredicate).isNotEqualTo(TupleDomain.none());
    }

    @Test
    public void testSimplifyPredicate()
    {
        ColumnHandle regularColumnHandle = prepareColumnHandle("regular", BIGINT, DeltaLakeColumnType.REGULAR);
        MetadataEntry metadataEntry = createMetadataEntry(
                ImmutableList.of(),
                """
                        {"fields":[{"name":"partitionedColumn","type":"long","nullable":false,"metadata":{}}]}"
                        """);

        TupleDomain<ColumnHandle> prunedPredicate = pageSourceProvider.getUnenforcedPredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L)))), ImmutableMap.of()),
                createDeltaLakeTableHandle(metadataEntry, TupleDomain.all()),
                TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L, 1L, 2L))))
        );
        assertThat(prunedPredicate).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.singleValue(BIGINT, 0L))));

        prunedPredicate = pageSourceProvider.getUnenforcedPredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.all(), ImmutableMap.of()),
                createDeltaLakeTableHandle(metadataEntry, TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L))))),
                TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L, 1L, 2L))))
        );
        assertThat(prunedPredicate).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.singleValue(BIGINT, 0L))));

        prunedPredicate = pageSourceProvider.getUnenforcedPredicate(
                TEST_SESSION.toConnectorSession(),
                prepareSplit(TupleDomain.all(), ImmutableMap.of()),
                createDeltaLakeTableHandle(metadataEntry, TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L, 1L))))),
                TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.multipleValues(BIGINT, ImmutableList.of(0L))))
        );
        assertThat(prunedPredicate).isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(regularColumnHandle, Domain.singleValue(BIGINT, 0L))));
    }

    private static ColumnHandle prepareColumnHandle(String name, Type type, DeltaLakeColumnType columnType)
    {
        return new DeltaLakeColumnHandle(
                name,
                type,
                OptionalInt.empty(),
                name,
                type,
                columnType,
                Optional.empty()
        );
    }

    private static DeltaLakeSplit prepareSplit(TupleDomain<ColumnHandle> statisticsPredicate, Map<String, Optional<String>> partitioningKeys)
    {
        return new DeltaLakeSplit(
                "",
                0,
                0,
                0,
                Optional.empty(),
                0,
                Optional.empty(),
                SplitWeight.standard(),
                statisticsPredicate.transformKeys(DeltaLakeColumnHandle.class::cast),
                partitioningKeys);
    }

    private static DeltaLakeTableHandle createDeltaLakeTableHandle(MetadataEntry metadataEntry, TupleDomain<ColumnHandle> nonPartitionConstraint)
    {
        return new DeltaLakeTableHandle(
                "schema",
                "table",
                false,
                "test_location",
                metadataEntry,
                new ProtocolEntry(1, 2, Optional.empty(), Optional.empty()),
                TupleDomain.all(),
                nonPartitionConstraint.transformKeys(DeltaLakeColumnHandle.class::cast),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                0);
    }

    private static MetadataEntry createMetadataEntry(List<String> partitionedColumns, String schema)
    {
        return new MetadataEntry(
                "test_id",
                "test_name",
                "test_description",
                new MetadataEntry.Format("test_provider", ImmutableMap.of()),
                schema,
                partitionedColumns,
                ImmutableMap.of(),
                1);
    }
}

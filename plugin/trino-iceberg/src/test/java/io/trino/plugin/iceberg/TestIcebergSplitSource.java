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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.cache.DefaultCachingHostAddressProvider;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.iceberg.catalog.rest.DefaultIcebergFileSystemFactory;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergSplitSource.createFileStatisticsDomain;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestIcebergSplitSource
        extends AbstractTestQueryFramework
{
    private static final ConnectorSession SESSION = TestingConnectorSession.builder()
            .setPropertyMetadata(new IcebergSessionProperties(
                    new IcebergConfig(),
                    new OrcReaderConfig(),
                    new OrcWriterConfig(),
                    new ParquetReaderConfig(),
                    new ParquetWriterConfig())
                    .getSessionProperties())
            .build();

    private File metastoreDir;
    private TrinoFileSystemFactory fileSystemFactory;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");

        QueryRunner queryRunner = IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();

        HiveMetastore metastore = ((IcebergConnector) queryRunner.getCoordinator().getConnector(ICEBERG_CATALOG)).getInjector()
                .getInstance(HiveMetastoreFactory.class)
                .createMetastore(Optional.empty());

        this.fileSystemFactory = getFileSystemFactory(queryRunner);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        this.catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                new FileMetastoreTableOperationsProvider(fileSystemFactory),
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable());

        return queryRunner;
    }

    @AfterAll
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Test
    @Timeout(30)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        long startMillis = System.currentTimeMillis();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", "nation");
        Table nationTable = catalog.loadTable(SESSION, schemaTableName);
        IcebergTableHandle tableHandle = createTableHandle(schemaTableName, nationTable, TupleDomain.all());

        try (IcebergSplitSource splitSource = new IcebergSplitSource(
                new DefaultIcebergFileSystemFactory(fileSystemFactory),
                SESSION,
                tableHandle,
                ImmutableMap.of(),
                nationTable.newScan(),
                Optional.empty(),
                new DynamicFilter()
                {
                    @Override
                    public Set<ColumnHandle> getColumnsCovered()
                    {
                        return ImmutableSet.of();
                    }

                    @Override
                    public CompletableFuture<?> isBlocked()
                    {
                        return CompletableFuture.runAsync(() -> {
                            try {
                                TimeUnit.HOURS.sleep(1);
                            }
                            catch (InterruptedException e) {
                                throw new IllegalStateException(e);
                            }
                        });
                    }

                    @Override
                    public boolean isComplete()
                    {
                        return false;
                    }

                    @Override
                    public boolean isAwaitable()
                    {
                        return true;
                    }

                    @Override
                    public TupleDomain<ColumnHandle> getCurrentPredicate()
                    {
                        return TupleDomain.all();
                    }
                },
                new Duration(2, SECONDS),
                alwaysTrue(),
                new TestingTypeManager(),
                false,
                new IcebergConfig().getMinimumAssignedSplitWeight(),
                new DefaultCachingHostAddressProvider())) {
            ImmutableList.Builder<IcebergSplit> splits = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splitSource.getNextBatch(100).get()
                        .getSplits()
                        .stream()
                        .map(IcebergSplit.class::cast)
                        .forEach(splits::add);
            }
            assertThat(splits.build().size()).isGreaterThan(0);
            assertThat(splitSource.isFinished()).isTrue();
            assertThat(System.currentTimeMillis() - startMillis)
                    .as("IcebergSplitSource failed to wait for dynamicFilteringWaitTimeout")
                    .isGreaterThanOrEqualTo(2000);
        }
    }

    @Test
    public void testFileStatisticsDomain()
            throws Exception
    {
        SchemaTableName schemaTableName = new SchemaTableName("tpch", "nation");
        Table nationTable = catalog.loadTable(SESSION, schemaTableName);
        IcebergTableHandle tableHandle = createTableHandle(schemaTableName, nationTable, TupleDomain.all());

        IcebergSplit split = generateSplit(nationTable, tableHandle, DynamicFilter.EMPTY);
        assertThat(split.getFileStatisticsDomain()).isEqualTo(TupleDomain.all());

        IcebergColumnHandle nationKey = new IcebergColumnHandle(
                new ColumnIdentity(1, "nationkey", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                true,
                Optional.empty());
        tableHandle = createTableHandle(schemaTableName, nationTable, TupleDomain.fromFixedValues(ImmutableMap.of(nationKey, NullableValue.of(BIGINT, 1L))));
        split = generateSplit(nationTable, tableHandle, DynamicFilter.EMPTY);
        assertThat(split.getFileStatisticsDomain()).isEqualTo(TupleDomain.withColumnDomains(
                ImmutableMap.of(nationKey, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 24L, true)), false))));

        IcebergColumnHandle regionKey = new IcebergColumnHandle(
                new ColumnIdentity(3, "regionkey", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                true,
                Optional.empty());
        split = generateSplit(nationTable, tableHandle, new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return ImmutableSet.of(regionKey);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return NOT_BLOCKED;
            }

            @Override
            public boolean isComplete()
            {
                return false;
            }

            @Override
            public boolean isAwaitable()
            {
                return true;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return TupleDomain.all();
            }
        });
        assertThat(split.getFileStatisticsDomain()).isEqualTo(TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        nationKey, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 24L, true)), false),
                        regionKey, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 4L, true)), false))));
    }

    @Test
    public void testBigintPartitionPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                true,
                Optional.empty());
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 100L))))).isFalse();
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L))))).isTrue();
        assertThat(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.asNull(BIGINT))))).isFalse();
    }

    @Test
    public void testBigintStatisticsPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                true,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));
        TupleDomain<IcebergColumnHandle> domainLowerUpperBound = TupleDomain.withColumnDomains(
                ImmutableMap.of(bigintColumn, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1000L, true, 2000L, true)), false)));
        List<IcebergColumnHandle> predicatedColumns = ImmutableList.of(bigintColumn);

        assertThat(createFileStatisticsDomain(primitiveTypes, lowerBound, upperBound, ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(domainLowerUpperBound);

        TupleDomain<IcebergColumnHandle> domainLowerUpperBoundAllowNulls = TupleDomain.withColumnDomains(
                ImmutableMap.of(bigintColumn, Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1000L, true, 2000L, true)), true)));
        assertThat(createFileStatisticsDomain(primitiveTypes, lowerBound, upperBound, ImmutableMap.of(1, 1L), predicatedColumns))
                .isEqualTo(domainLowerUpperBoundAllowNulls);
    }

    @Test
    public void testNullStatisticsMaps()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                true,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), -1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));
        TupleDomain<IcebergColumnHandle> domainLessThanUpperBound = TupleDomain.withColumnDomains(
                ImmutableMap.of(bigintColumn, Domain.create(ValueSet.ofRanges(Range.lessThanOrEqual(BIGINT, 2000L)), false)));
        List<IcebergColumnHandle> predicatedColumns = ImmutableList.of(bigintColumn);

        assertThat(createFileStatisticsDomain(primitiveTypes, null, upperBound, ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(domainLessThanUpperBound);
        assertThat(createFileStatisticsDomain(primitiveTypes, ImmutableMap.of(), upperBound, ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(domainLessThanUpperBound);

        TupleDomain<IcebergColumnHandle> domainGreaterThanLessBound = TupleDomain.withColumnDomains(
                ImmutableMap.of(bigintColumn, Domain.create(ValueSet.ofRanges(Range.greaterThanOrEqual(BIGINT, -1000L)), false)));
        assertThat(createFileStatisticsDomain(primitiveTypes, lowerBound, null, ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(domainGreaterThanLessBound);
        assertThat(createFileStatisticsDomain(primitiveTypes, lowerBound, ImmutableMap.of(), ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(domainGreaterThanLessBound);

        assertThat(createFileStatisticsDomain(primitiveTypes, ImmutableMap.of(), ImmutableMap.of(), null, predicatedColumns))
                .isEqualTo(TupleDomain.all());
        assertThat(createFileStatisticsDomain(primitiveTypes, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(), predicatedColumns))
                .isEqualTo(TupleDomain.all());
        assertThat(createFileStatisticsDomain(primitiveTypes, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(1, 1L), predicatedColumns))
                .isEqualTo(TupleDomain.all());

        assertThat(createFileStatisticsDomain(primitiveTypes, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of(1, 0L), predicatedColumns))
                .isEqualTo(TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, Domain.notNull(BIGINT))));
    }

    private IcebergSplit generateSplit(Table nationTable, IcebergTableHandle tableHandle, DynamicFilter dynamicFilter)
            throws Exception
    {
        try (IcebergSplitSource splitSource = new IcebergSplitSource(
                new DefaultIcebergFileSystemFactory(fileSystemFactory),
                SESSION,
                tableHandle,
                ImmutableMap.of(),
                nationTable.newScan(),
                Optional.empty(),
                dynamicFilter,
                new Duration(0, SECONDS),
                alwaysTrue(),
                new TestingTypeManager(),
                false,
                new IcebergConfig().getMinimumAssignedSplitWeight(),
                new DefaultCachingHostAddressProvider())) {
            ImmutableList.Builder<IcebergSplit> builder = ImmutableList.builder();
            while (!splitSource.isFinished()) {
                splitSource.getNextBatch(100).get()
                        .getSplits()
                        .stream()
                        .map(IcebergSplit.class::cast)
                        .forEach(builder::add);
            }
            List<IcebergSplit> splits = builder.build();
            assertThat(splits.size()).isEqualTo(1);
            assertThat(splitSource.isFinished()).isTrue();

            return splits.getFirst();
        }
    }

    private static IcebergTableHandle createTableHandle(SchemaTableName schemaTableName, Table nationTable, TupleDomain<IcebergColumnHandle> unenforcedPredicate)
    {
        return new IcebergTableHandle(
                CatalogHandle.fromId("iceberg:NORMAL:v12345"),
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                Optional.empty(),
                SchemaParser.toJson(nationTable.schema()),
                Optional.of(PartitionSpecParser.toJson(nationTable.spec())),
                1,
                unenforcedPredicate,
                TupleDomain.all(),
                OptionalLong.empty(),
                ImmutableSet.of(),
                Optional.empty(),
                nationTable.location(),
                nationTable.properties(),
                false,
                Optional.empty(),
                ImmutableSet.of(),
                Optional.of(false));
    }
}

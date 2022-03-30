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
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.spi.connector.ColumnHandle;
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
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.spi.connector.Constraint.alwaysTrue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.tpch.TpchTable.NATION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIcebergSplitSource
        extends AbstractTestQueryFramework
{
    private File metastoreDir;
    private TrinoCatalog catalog;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());

        File tempDir = Files.createTempDirectory("test_iceberg_split_source").toFile();
        this.metastoreDir = new File(tempDir, "iceberg_data");
        HiveMetastore metastore = createTestingFileHiveMetastore(metastoreDir);
        IcebergTableOperationsProvider operationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));
        this.catalog = new TrinoHiveCatalog(
                new CatalogName("hive"),
                memoizeMetastore(metastore, 1000),
                hdfsEnvironment,
                new TestingTypeManager(),
                operationsProvider,
                "test",
                false,
                false,
                false);

        return IcebergQueryRunner.builder()
                .setInitialTables(NATION)
                .setMetastoreDirectory(metastoreDir)
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(metastoreDir.getParentFile().toPath(), ALLOW_INSECURE);
    }

    @Test(timeOut = 30_000)
    public void testIncompleteDynamicFilterTimeout()
            throws Exception
    {
        long startMillis = System.currentTimeMillis();
        SchemaTableName schemaTableName = new SchemaTableName("tpch", "nation");
        IcebergTableHandle tableHandle = new IcebergTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                TableType.DATA,
                Optional.empty(),
                TupleDomain.all(),
                TupleDomain.all(),
                ImmutableSet.of(),
                Optional.empty());
        Table nationTable = catalog.loadTable(SESSION, schemaTableName);

        IcebergSplitSource splitSource = new IcebergSplitSource(
                tableHandle,
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
                false);

        ImmutableList.Builder<IcebergSplit> splits = ImmutableList.builder();
        while (!splitSource.isFinished()) {
            splitSource.getNextBatch(null, 100).get()
                    .getSplits()
                    .stream()
                    .map(IcebergSplit.class::cast)
                    .forEach(splits::add);
        }
        assertThat(splits.build().size()).isGreaterThan(0);
        assertTrue(splitSource.isFinished());
        assertThat(System.currentTimeMillis() - startMillis)
                .as("IcebergSplitSource failed to wait for dynamicFilteringWaitTimeout")
                .isGreaterThanOrEqualTo(2000);
    }

    @Test
    public void testBigintPartitionPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        assertFalse(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 100L)))));
        assertTrue(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)))));
        assertFalse(IcebergSplitSource.partitionMatchesPredicate(
                ImmutableSet.of(bigintColumn),
                () -> ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L)),
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.asNull(BIGINT)))));
    }

    @Test
    public void testBigintStatisticsPruning()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));

        assertFalse(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 0L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 1500L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 2000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertFalse(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 3000L))),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));

        Domain outsideStatisticsRangeAllowNulls = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 100L, true)), true);
        assertFalse(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeAllowNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeAllowNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L)));

        Domain outsideStatisticsRangeNoNulls = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 0L, true, 100L, true)), false);
        assertFalse(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeNoNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertFalse(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, outsideStatisticsRangeNoNulls)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L)));

        Domain insideStatisticsRange = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 1001L, true, 1002L, true)), false);
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, insideStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, insideStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L)));

        Domain overlappingStatisticsRange = Domain.create(ValueSet.ofRanges(Range.range(BIGINT, 990L, true, 1010L, true)), false);
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, overlappingStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, overlappingStatisticsRange)),
                lowerBound,
                upperBound,
                ImmutableMap.of(1, 1L)));
    }

    @Test
    public void testNullStatisticsMaps()
    {
        IcebergColumnHandle bigintColumn = new IcebergColumnHandle(
                new ColumnIdentity(1, "name", ColumnIdentity.TypeCategory.PRIMITIVE, ImmutableList.of()),
                BIGINT,
                ImmutableList.of(),
                BIGINT,
                Optional.empty());
        Map<Integer, Type.PrimitiveType> primitiveTypes = ImmutableMap.of(1, Types.LongType.get());
        Map<Integer, ByteBuffer> lowerBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), -1000L));
        Map<Integer, ByteBuffer> upperBound = ImmutableMap.of(1, Conversions.toByteBuffer(Types.LongType.get(), 2000L));
        TupleDomain<IcebergColumnHandle> domainOfZero = TupleDomain.fromFixedValues(ImmutableMap.of(bigintColumn, NullableValue.of(BIGINT, 0L)));

        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                null,
                upperBound,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                ImmutableMap.of(),
                upperBound,
                ImmutableMap.of(1, 0L)));

        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                lowerBound,
                null,
                ImmutableMap.of(1, 0L)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                domainOfZero,
                lowerBound,
                ImmutableMap.of(),
                ImmutableMap.of(1, 0L)));

        TupleDomain<IcebergColumnHandle> onlyNull = TupleDomain.withColumnDomains(ImmutableMap.of(bigintColumn, Domain.onlyNull(BIGINT)));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                onlyNull,
                ImmutableMap.of(),
                ImmutableMap.of(),
                null));
        assertTrue(IcebergSplitSource.fileMatchesPredicate(
                primitiveTypes,
                onlyNull,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of()));
    }
}

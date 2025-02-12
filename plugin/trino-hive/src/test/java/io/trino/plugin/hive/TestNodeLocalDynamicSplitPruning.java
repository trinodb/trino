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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.metadata.TableHandle;
import io.trino.metastore.Column;
import io.trino.plugin.hive.orc.OrcReaderConfig;
import io.trino.plugin.hive.orc.OrcWriterConfig;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hive.parquet.ParquetWriterConfig;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.EmptyPageSource;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveTestUtils.getDefaultHivePageSourceFactories;
import static io.trino.plugin.hive.util.HiveBucketing.BucketingVersion.BUCKETING_V1;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.TestingHandles.TEST_CATALOG_HANDLE;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.assertj.core.api.Assertions.assertThat;

class TestNodeLocalDynamicSplitPruning
{
    private static final String SCHEMA_NAME = "test";
    private static final String TABLE_NAME = "test";
    private static final Column BUCKET_COLUMN = new Column("l_orderkey", HIVE_INT, Optional.empty(), ImmutableMap.of());
    private static final Column PARTITION_COLUMN = new Column("l_partkey", HIVE_INT, Optional.empty(), ImmutableMap.of());
    private static final HiveColumnHandle BUCKET_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            BUCKET_COLUMN.getName(),
            0,
            BUCKET_COLUMN.getType(),
            INTEGER,
            Optional.empty(),
            REGULAR,
            Optional.empty());
    private static final HiveColumnHandle PARTITION_HIVE_COLUMN_HANDLE = new HiveColumnHandle(
            PARTITION_COLUMN.getName(),
            0,
            PARTITION_COLUMN.getType(),
            INTEGER,
            Optional.empty(),
            PARTITION_KEY,
            Optional.empty());

    @Test
    void testDynamicBucketPruning()
            throws IOException
    {
        HiveConfig config = new HiveConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);
        try (ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, config, getDynamicFilter(getTupleDomainForBucketSplitPruning()))) {
            assertThat(emptyPageSource.getClass()).isEqualTo(EmptyPageSource.class);
        }

        try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, config, getDynamicFilter(getNonSelectiveBucketTupleDomain()))) {
            assertThat(nonEmptyPageSource.getClass()).isEqualTo(HivePageSource.class);
        }
    }

    @Test
    void testDynamicPartitionPruning()
            throws IOException
    {
        HiveConfig config = new HiveConfig();
        HiveTransactionHandle transaction = new HiveTransactionHandle(false);

        try (ConnectorPageSource emptyPageSource = createTestingPageSource(transaction, config, getDynamicFilter(getTupleDomainForPartitionSplitPruning()))) {
            assertThat(emptyPageSource.getClass()).isEqualTo(EmptyPageSource.class);
        }

        try (ConnectorPageSource nonEmptyPageSource = createTestingPageSource(transaction, config, getDynamicFilter(getNonSelectivePartitionTupleDomain()))) {
            assertThat(nonEmptyPageSource.getClass()).isEqualTo(HivePageSource.class);
        }
    }

    private static ConnectorPageSource createTestingPageSource(HiveTransactionHandle transaction, HiveConfig hiveConfig, DynamicFilter dynamicFilter)
            throws IOException
    {
        Location location = Location.of("memory:///file");
        TrinoFileSystemFactory fileSystemFactory = new MemoryFileSystemFactory();
        fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newOutputFile(location).create().close();

        HiveSplit split = new HiveSplit(
                "",
                location.toString(),
                0,
                0,
                0,
                0,
                new Schema(hiveConfig.getHiveStorageFormat().getSerde(), false, ImmutableMap.of()),
                ImmutableList.of(new HivePartitionKey(PARTITION_COLUMN.getName(), "42")),
                ImmutableList.of(),
                OptionalInt.of(1),
                OptionalInt.of(1),
                false,
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                SplitWeight.standard());

        TableHandle tableHandle = new TableHandle(
                TEST_CATALOG_HANDLE,
                new HiveTableHandle(
                        SCHEMA_NAME,
                        TABLE_NAME,
                        ImmutableMap.of(),
                        ImmutableList.of(),
                        ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                        Optional.of(new HiveTablePartitioning(
                                true,
                                BUCKETING_V1,
                                20,
                                ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE),
                                false,
                                ImmutableList.of(),
                                true))),
                transaction);

        HivePageSourceProvider provider = new HivePageSourceProvider(
                TESTING_TYPE_MANAGER,
                hiveConfig,
                getDefaultHivePageSourceFactories(fileSystemFactory, hiveConfig));

        return provider.createPageSource(
                transaction,
                getSession(hiveConfig),
                split,
                tableHandle.connectorHandle(),
                ImmutableList.of(BUCKET_HIVE_COLUMN_HANDLE, PARTITION_HIVE_COLUMN_HANDLE),
                dynamicFilter);
    }

    private static TupleDomain<ColumnHandle> getTupleDomainForBucketSplitPruning()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        BUCKET_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 10L)));
    }

    private static TupleDomain<ColumnHandle> getNonSelectiveBucketTupleDomain()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        BUCKET_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 1L)));
    }

    private static TupleDomain<ColumnHandle> getTupleDomainForPartitionSplitPruning()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        PARTITION_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 1L)));
    }

    private static TupleDomain<ColumnHandle> getNonSelectivePartitionTupleDomain()
    {
        return TupleDomain.withColumnDomains(
                ImmutableMap.of(
                        PARTITION_HIVE_COLUMN_HANDLE,
                        Domain.singleValue(INTEGER, 42L)));
    }

    private static TestingConnectorSession getSession(HiveConfig config)
    {
        return TestingConnectorSession.builder()
                .setPropertyMetadata(new HiveSessionProperties(
                        config,
                        new OrcReaderConfig(),
                        new OrcWriterConfig(),
                        new ParquetReaderConfig(),
                        new ParquetWriterConfig()).getSessionProperties())
                .build();
    }

    private static DynamicFilter getDynamicFilter(TupleDomain<ColumnHandle> tupleDomain)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return tupleDomain.getDomains().map(Map::keySet)
                        .orElseGet(ImmutableSet::of);
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return tupleDomain;
            }
        };
    }
}

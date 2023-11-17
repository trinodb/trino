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
package io.trino.plugin.hive.metastore.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.hive.thrift.metastore.ColumnStatisticsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.UnimplementedHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.BridgingHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftHiveMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastore;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreClient;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreStats;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.BAD_DATABASE;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.BAD_PARTITION;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.PARTITION_COLUMN_NAMES;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_COLUMN;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_DATABASE;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION1;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION1_VALUE;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION2;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION3;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION_VALUES1;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION_VALUES2;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION_VALUES3;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_ROLES;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_TABLE;
import static io.trino.spi.predicate.TupleDomain.withColumnDomains;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestCachingHiveMetastore
{
    private static final Logger log = Logger.get(TestCachingHiveMetastore.class);

    private static final PartitionStatistics TEST_STATS = PartitionStatistics.builder()
            .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(2398040535435L), OptionalLong.empty(), OptionalLong.empty()))
            .setColumnStatistics(ImmutableMap.of(TEST_COLUMN, createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty())))
            .build();
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_DATABASE, TEST_TABLE);
    private static final Duration CACHE_TTL = new Duration(5, TimeUnit.MINUTES);

    private MockThriftMetastoreClient mockClient;
    private ThriftMetastore thriftHiveMetastore;
    private ListeningExecutorService executor;
    private CachingHiveMetastore metastore;
    private CachingHiveMetastore statsOnlyCacheMetastore;
    private ThriftMetastoreStats stats;

    @BeforeEach
    public void setUp()
    {
        mockClient = new MockThriftMetastoreClient();
        thriftHiveMetastore = createThriftHiveMetastore();
        executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s")));

        metastore = createCachingHiveMetastore(new BridgingHiveMetastore(thriftHiveMetastore), CACHE_TTL, true, true, executor);
        statsOnlyCacheMetastore = createCachingHiveMetastore(new BridgingHiveMetastore(thriftHiveMetastore), Duration.ZERO, true, true, executor);

        stats = ((ThriftHiveMetastore) thriftHiveMetastore).getStats();
    }

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        executor = null;
        metastore = null;
    }

    private ThriftMetastore createThriftHiveMetastore()
    {
        return createThriftHiveMetastore(mockClient);
    }

    private static ThriftMetastore createThriftHiveMetastore(ThriftMetastoreClient client)
    {
        return testingThriftHiveMetastoreBuilder()
                .metastoreClient(client)
                .build();
    }

    @Test
    public void testCachingWithOnlyPartitionsCacheEnabled()
    {
        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(CachingHiveMetastore::getAllDatabases)
                .usesCache();

        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(testedMetastore -> testedMetastore.getAllTables(TEST_DATABASE))
                .usesCache();

        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(testedMetastore -> testedMetastore.getTable(TEST_DATABASE, TEST_TABLE))
                .usesCache();

        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(testedMetastore -> testedMetastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()))
                .doesNotUseCache();

        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(testedMetastore -> {
                    Optional<Table> table = testedMetastore.getTable(TEST_DATABASE, TEST_TABLE);
                    testedMetastore.getPartition(table.orElseThrow(), TEST_PARTITION_VALUES1);
                })
                .omitsCacheForNumberOfOperations(1);

        assertThatCachingWithDisabledPartitionCache()
                .whenExecuting(testedMetastore -> {
                    Optional<Table> table = testedMetastore.getTable(TEST_DATABASE, TEST_TABLE);
                    testedMetastore.getPartitionsByNames(table.orElseThrow(), TEST_PARTITION_VALUES1);
                })
                .omitsCacheForNumberOfOperations(1);
    }

    @Test
    public void testGetAllDatabases()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getAllDatabases()).isEqualTo(ImmutableList.of(TEST_DATABASE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllDatabases()).isEqualTo(ImmutableList.of(TEST_DATABASE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getDatabaseNamesStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getDatabaseNamesStats().getHitRate()).isEqualTo(0.5);

        metastore.flushCache();

        assertThat(metastore.getAllDatabases()).isEqualTo(ImmutableList.of(TEST_DATABASE));
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getDatabaseNamesStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getDatabaseNamesStats().getHitRate()).isEqualTo(1.0 / 3);
    }

    @Test
    public void testGetAllTable()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getAllTables(TEST_DATABASE)).isEqualTo(ImmutableList.of(TEST_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllTables(TEST_DATABASE)).isEqualTo(ImmutableList.of(TEST_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getTableNamesStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getTableNamesStats().getHitRate()).isEqualTo(0.5);

        metastore.flushCache();

        assertThat(metastore.getAllTables(TEST_DATABASE)).isEqualTo(ImmutableList.of(TEST_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getTableNamesStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getTableNamesStats().getHitRate()).isEqualTo(1.0 / 3);
    }

    @Test
    public void testBatchGetAllTable()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getAllTables()).isEqualTo(Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllTables()).isEqualTo(Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllTables(TEST_DATABASE)).isEqualTo(ImmutableList.of(TEST_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getAllTableNamesStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getAllTableNamesStats().getHitRate()).isEqualTo(.5);

        metastore.flushCache();

        assertThat(metastore.getAllTables()).isEqualTo(Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertThat(mockClient.getAccessCount()).isEqualTo(3);
        assertThat(metastore.getAllTableNamesStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getAllTableNamesStats().getHitRate()).isEqualTo(1. / 3);
    }

    @Test
    public void testInvalidDbGetAllTAbles()
    {
        assertThat(metastore.getAllTables(BAD_DATABASE).isEmpty()).isTrue();
    }

    @Test
    public void testGetTable()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getTableStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getTableStats().getHitRate()).isEqualTo(0.5);

        metastore.flushCache();

        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getTableStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getTableStats().getHitRate()).isEqualTo(1.0 / 3);
    }

    @Test
    public void testSetTableAuthorization()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(metastore.getDatabase(TEST_DATABASE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        metastore.setTableOwner(TEST_DATABASE, TEST_TABLE, new HivePrincipal(USER, "ignore"));
        assertThat(mockClient.getAccessCount()).isEqualTo(3);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
        // Assert that database cache has not been invalidated
        assertThat(metastore.getDatabase(TEST_DATABASE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
    }

    @Test
    public void testInvalidDbGetTable()
    {
        assertThat(metastore.getTable(BAD_DATABASE, TEST_TABLE).isPresent()).isFalse();

        assertThat(stats.getGetTable().getThriftExceptions().getTotalCount()).isEqualTo(0);
        assertThat(stats.getGetTable().getTotalFailures().getTotalCount()).isEqualTo(0);
    }

    @Test
    public void testGetPartitionNames()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        metastore.flushCache();

        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
    }

    /**
     * Test {@link CachingHiveMetastore#getPartition(Table, List)} followed by
     * {@link CachingHiveMetastore#getPartitionsByNames(Table, List)}.
     * <p>
     * At the moment of writing, CachingHiveMetastore uses HivePartitionName for keys in partition cache.
     * HivePartitionName has a peculiar, semi- value-based equality. HivePartitionName may or may not be missing
     * a name, and it matters for bulk load, but it doesn't matter for single-partition load.
     * Because of equality semantics, the cache keys may get mixed during bulk load.
     */
    @Test
    public void testGetPartitionThenGetPartitions()
    {
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        Optional<Partition> firstRead = metastore.getPartition(table, List.of(TEST_PARTITION1_VALUE));
        assertThat(firstRead).isPresent();
        assertThat(firstRead.get().getValues()).isEqualTo(List.of(TEST_PARTITION1_VALUE));

        Map<String, Optional<Partition>> byName = metastore.getPartitionsByNames(table, List.of(TEST_PARTITION1));
        assertThat(byName).containsOnlyKeys(TEST_PARTITION1);
        Optional<Partition> secondRead = byName.get(TEST_PARTITION1);
        assertThat(secondRead).isPresent();
        assertThat(secondRead.get().getValues()).isEqualTo(List.of(TEST_PARTITION1_VALUE));
    }

    /**
     * A variant of {@link #testGetPartitionThenGetPartitions} where the second get happens concurrently with eviction,
     * here simulated with an explicit invalidation.
     */
    // Repeat test with invocationCount for better test coverage, since the tested aspect is inherently non-deterministic.
    @RepeatedTest(20)
    @Timeout(60)
    public void testGetPartitionThenGetPartitionsRacingWithInvalidation()
            throws Exception
    {
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        Optional<Partition> firstRead = metastore.getPartition(table, List.of(TEST_PARTITION1_VALUE));
        assertThat(firstRead).isPresent();
        assertThat(firstRead.get().getValues()).isEqualTo(List.of(TEST_PARTITION1_VALUE));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            CyclicBarrier barrier = new CyclicBarrier(2);
            Future<?> invalidation = executor.submit(() -> {
                barrier.await(10, SECONDS);
                metastore.flushCache();
                return null;
            });

            Future<Map<String, Optional<Partition>>> read = executor.submit(() -> {
                barrier.await(10, SECONDS);
                return metastore.getPartitionsByNames(table, List.of(TEST_PARTITION1));
            });

            Map<String, Optional<Partition>> byName = read.get();
            assertThat(byName).containsOnlyKeys(TEST_PARTITION1);
            Optional<Partition> secondRead = byName.get(TEST_PARTITION1);
            assertThat(secondRead).isPresent();
            assertThat(secondRead.get().getValues()).isEqualTo(List.of(TEST_PARTITION1_VALUE));

            invalidation.get(); // no exception raised
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testInvalidGetPartitionNamesByFilterAll()
    {
        assertThat(metastore.getPartitionNamesByFilter(BAD_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).isEmpty()).isTrue();
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);

        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getPartitionFilterStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getPartitionFilterStats().getHitRate()).isEqualTo(0.5);

        metastore.flushCache();

        assertThat(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow()).isEqualTo(expectedPartitions);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getPartitionFilterStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getPartitionFilterStats().getHitRate()).isEqualTo(1.0 / 3);

        List<String> partitionColumnNames = ImmutableList.of("date_key", "key");
        HiveColumnHandle dateKeyColumn = createBaseColumn(partitionColumnNames.get(0), 0, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
        HiveColumnHandle keyColumn = createBaseColumn(partitionColumnNames.get(1), 1, HIVE_STRING, VARCHAR, PARTITION_KEY, Optional.empty());
        List<HiveColumnHandle> partitionColumns = ImmutableList.of(dateKeyColumn, keyColumn);

        TupleDomain<String> withNoFilter = computePartitionKeyFilter(
                partitionColumns,
                TupleDomain.all());

        TupleDomain<String> withSingleValueFilter = computePartitionKeyFilter(
                partitionColumns,
                withColumnDomains(ImmutableMap.<HiveColumnHandle, Domain>builder()
                        .put(dateKeyColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("2020-10-01"))), false))
                        .put(keyColumn, Domain.create(ValueSet.of(VARCHAR, utf8Slice("val")), false))
                        .buildOrThrow()));

        TupleDomain<String> withNoSingleValueFilter = computePartitionKeyFilter(
                partitionColumns,
                withColumnDomains(ImmutableMap.<HiveColumnHandle, Domain>builder()
                        .put(dateKeyColumn, Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, utf8Slice("2020-10-01"))), false))
                        .put(keyColumn, Domain.create(ValueSet.ofRanges(Range.range(VARCHAR, utf8Slice("val1"), true, utf8Slice("val2"), true)), false))
                        .buildOrThrow()));

        assertThat(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount()).isEqualTo(0.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withNoFilter);
        assertThat(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount()).isEqualTo(0.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withSingleValueFilter);
        assertThat(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount()).isEqualTo(1.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withNoSingleValueFilter);
        assertThat(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount()).isEqualTo(2.0);
    }

    @Test
    public void testInvalidGetPartitionNamesByParts()
    {
        assertThat(metastore.getPartitionNamesByFilter(BAD_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).isPresent()).isFalse();
    }

    @Test
    public void testGetPartitionsByNames()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        // Select half of the available partitions and load them into the cache
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1)).size()).isEqualTo(1);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        // Now select all the partitions
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size()).isEqualTo(2);
        // There should be one more access to fetch the remaining partition
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        // Now if we fetch any or both of them, they should not hit the client
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1)).size()).isEqualTo(1);
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION2)).size()).isEqualTo(1);
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size()).isEqualTo(2);
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        metastore.flushCache();

        // Fetching both should only result in one batched access
        assertThat(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size()).isEqualTo(2);
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
    }

    @Test
    public void testListRoles()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        assertThat(metastore.listRoles()).containsExactlyElementsOf(TEST_ROLES);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        assertThat(metastore.listRoles()).containsExactlyElementsOf(TEST_ROLES);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        assertThat(metastore.getRolesStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getRolesStats().getHitRate()).isEqualTo(0.5);

        metastore.flushCache();

        assertThat(metastore.listRoles()).containsExactlyElementsOf(TEST_ROLES);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        metastore.createRole("role", "grantor");

        assertThat(metastore.listRoles()).containsExactlyElementsOf(TEST_ROLES);
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        metastore.dropRole("testrole");

        assertThat(metastore.listRoles()).containsExactlyElementsOf(TEST_ROLES);
        assertThat(mockClient.getAccessCount()).isEqualTo(4);

        assertThat(metastore.getRolesStats().getRequestCount()).isEqualTo(5);
        assertThat(metastore.getRolesStats().getHitRate()).isEqualTo(0.2);
    }

    @Test
    public void testGetTableStatistics()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        assertThat(metastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        assertThat(metastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        assertThat(metastore.getTableStatisticsStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getTableStatisticsStats().getHitRate()).isEqualTo(0.5);

        assertThat(metastore.getTableStats().getRequestCount()).isEqualTo(1);
        assertThat(metastore.getTableStats().getHitRate()).isEqualTo(0.0);

        // check empty column list does not trigger the call
        Table emptyColumnListTable = Table.builder(table).setDataColumns(ImmutableList.of()).build();
        assertThat(metastore.getTableStatistics(emptyColumnListTable).getBasicStatistics()).isEqualTo(TEST_STATS.getBasicStatistics());
        assertThat(metastore.getTableStatisticsStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getTableStatisticsStats().getHitRate()).isEqualTo(2.0 / 3);

        mockClient.mockColumnStats(TEST_DATABASE, TEST_TABLE, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(1)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(2)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(3))));
        Table tableCol1 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty(), Map.of()))).build();
        assertThat(metastore.getTableStatistics(tableCol1).getColumnStatistics()).containsEntry("col1", intColumnStats(1));
        Table tableCol2 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty(), Map.of()))).build();
        assertThat(metastore.getTableStatistics(tableCol2).getColumnStatistics()).containsEntry("col2", intColumnStats(2));
        Table tableCol23 = Table.builder(table)
                .setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty(), Map.of()), new Column("col3", HIVE_LONG, Optional.empty(), Map.of())))
                .build();
        assertThat(metastore.getTableStatistics(tableCol23).getColumnStatistics())
                .containsEntry("col2", intColumnStats(2))
                .containsEntry("col3", intColumnStats(3));

        metastore.getTableStatistics(table); // ensure cached
        assertThat(mockClient.getAccessCount()).isEqualTo(5);
        ColumnStatisticsData newStats = new ColumnStatisticsData();
        newStats.setLongStats(new LongColumnStatsData(327843, 4324));
        mockClient.mockColumnStats(TEST_DATABASE, TEST_TABLE, ImmutableMap.of(TEST_COLUMN, newStats));
        metastore.invalidateTable(TEST_DATABASE, TEST_TABLE);
        assertThat(metastore.getTableStatistics(table)).isEqualTo(PartitionStatistics.builder()
                .setBasicStatistics(TEST_STATS.getBasicStatistics())
                .setColumnStatistics(ImmutableMap.of(TEST_COLUMN, createIntegerColumnStatistics(
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        OptionalLong.of(newStats.getLongStats().getNumNulls()),
                        OptionalLong.of(newStats.getLongStats().getNumDVs() - 1))))
                .build());
        assertThat(mockClient.getAccessCount()).isEqualTo(6);
    }

    @Test
    public void testGetTableStatisticsWithoutMetadataCache()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        Table table = statsOnlyCacheMetastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        assertThat(statsOnlyCacheMetastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        assertThat(statsOnlyCacheMetastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        assertThat(statsOnlyCacheMetastore.getTableStatisticsStats().getRequestCount()).isEqualTo(2);
        assertThat(statsOnlyCacheMetastore.getTableStatisticsStats().getHitRate()).isEqualTo(0.5);

        assertThat(statsOnlyCacheMetastore.getTableStats().getRequestCount()).isEqualTo(0);
        assertThat(statsOnlyCacheMetastore.getTableStats().getHitRate()).isEqualTo(1.0);
    }

    @Test
    public void testInvalidateWithLoadInProgress()
            throws Exception
    {
        CountDownLatch loadInProgress = new CountDownLatch(1);
        CountDownLatch invalidateDone = new CountDownLatch(1);
        MockThriftMetastoreClient mockClient = new MockThriftMetastoreClient()
        {
            @Override
            public List<ColumnStatisticsObj> getTableColumnStatistics(String databaseName, String tableName, List<String> columnNames)
                    throws TException
            {
                loadInProgress.countDown();
                List<ColumnStatisticsObj> result = super.getTableColumnStatistics(databaseName, tableName, columnNames);
                // get should wait for the invalidation to be done to return result
                try {
                    invalidateDone.await(10, SECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return result;
            }
        };
        CachingHiveMetastore metastore = createCachingHiveMetastore(new BridgingHiveMetastore(createThriftHiveMetastore(mockClient)), CACHE_TTL, true, true, executor);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();

        ExecutorService executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("invalidation-%d").build());
        try {
            // invalidate thread
            Future<?> invalidateFuture = executorService.submit(
                    () -> {
                        try {
                            loadInProgress.await(10, SECONDS);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        metastore.flushCache();
                        invalidateDone.countDown();
                    });

            // start get stats before the invalidation, it will wait until invalidation is done to finish
            assertThat(metastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
            assertThat(mockClient.getAccessCount()).isEqualTo(2);
            // get stats after invalidate
            assertThat(metastore.getTableStatistics(table)).isEqualTo(TEST_STATS);
            // the value was not cached
            assertThat(mockClient.getAccessCount()).isEqualTo(3);
            // make sure invalidateFuture is done
            invalidateFuture.get(1, SECONDS);
        }
        finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testGetPartitionStatistics()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        Partition partition = metastore.getPartition(table, TEST_PARTITION_VALUES1).orElseThrow();
        String partitionName = makePartitionName(table, partition);
        Partition partition2 = metastore.getPartition(table, TEST_PARTITION_VALUES2).orElseThrow();
        String partition2Name = makePartitionName(table, partition2);
        Partition partition3 = metastore.getPartition(table, TEST_PARTITION_VALUES3).orElseThrow();
        String partition3Name = makePartitionName(table, partition3);
        assertThat(mockClient.getAccessCount()).isEqualTo(4);

        assertThat(metastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertThat(mockClient.getAccessCount()).isEqualTo(5);

        assertThat(metastore.getPartitionStatisticsStats().getRequestCount()).isEqualTo(1);
        assertThat(metastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertThat(mockClient.getAccessCount()).isEqualTo(5);

        assertThat(metastore.getPartitionStatisticsStats().getRequestCount()).isEqualTo(2);
        assertThat(metastore.getPartitionStatisticsStats().getHitRate()).isEqualTo(1.0 / 2);

        assertThat(metastore.getTableStats().getRequestCount()).isEqualTo(1);
        assertThat(metastore.getTableStats().getHitRate()).isEqualTo(0.0);

        assertThat(metastore.getPartitionStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getPartitionStats().getHitRate()).isEqualTo(0.0);

        // check empty column list does not trigger the call
        Table emptyColumnListTable = Table.builder(table).setDataColumns(ImmutableList.of()).build();
        Map<String, PartitionStatistics> partitionStatistics = metastore.getPartitionStatistics(emptyColumnListTable, ImmutableList.of(partition));
        assertThat(partitionStatistics).containsOnlyKeys(TEST_PARTITION1);
        assertThat(partitionStatistics.get(TEST_PARTITION1).getBasicStatistics()).isEqualTo(TEST_STATS.getBasicStatistics());
        assertThat(metastore.getPartitionStatisticsStats().getRequestCount()).isEqualTo(3);
        assertThat(metastore.getPartitionStatisticsStats().getHitRate()).isEqualTo(2.0 / 3);

        mockClient.mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION1, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(1)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(2)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(3))));

        Table tableCol1 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty(), Map.of()))).build();
        Map<String, PartitionStatistics> tableCol1PartitionStatistics = metastore.getPartitionStatistics(tableCol1, ImmutableList.of(partition));
        assertThat(tableCol1PartitionStatistics).containsOnlyKeys(partitionName);
        assertThat(tableCol1PartitionStatistics.get(partitionName).getColumnStatistics()).containsEntry("col1", intColumnStats(1));
        Table tableCol2 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty(), Map.of()))).build();
        Map<String, PartitionStatistics> tableCol2PartitionStatistics = metastore.getPartitionStatistics(tableCol2, ImmutableList.of(partition));
        assertThat(tableCol2PartitionStatistics).containsOnlyKeys(partitionName);
        assertThat(tableCol2PartitionStatistics.get(partitionName).getColumnStatistics()).containsEntry("col2", intColumnStats(2));
        Table tableCol23 = Table.builder(table)
                .setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty(), Map.of()), new Column("col3", HIVE_LONG, Optional.empty(), Map.of())))
                .build();
        Map<String, PartitionStatistics> tableCol23PartitionStatistics = metastore.getPartitionStatistics(tableCol23, ImmutableList.of(partition));
        assertThat(tableCol23PartitionStatistics).containsOnlyKeys(partitionName);
        assertThat(tableCol23PartitionStatistics.get(partitionName).getColumnStatistics())
                .containsEntry("col2", intColumnStats(2))
                .containsEntry("col3", intColumnStats(3));

        mockClient.mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION2, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(21)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(22)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(23))));

        mockClient.mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION3, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(31)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(32)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(33))));

        Map<String, PartitionStatistics> tableCol2Partition2Statistics = metastore.getPartitionStatistics(tableCol2, ImmutableList.of(partition2));
        assertThat(tableCol2Partition2Statistics).containsOnlyKeys(partition2Name);
        assertThat(tableCol2Partition2Statistics.get(partition2Name).getColumnStatistics()).containsEntry("col2", intColumnStats(22));

        Map<String, PartitionStatistics> tableCol23Partition123Statistics = metastore.getPartitionStatistics(tableCol23, ImmutableList.of(partition, partition2, partition3));
        assertThat(tableCol23Partition123Statistics).containsOnlyKeys(partitionName, partition2Name, partition3Name);
        assertThat(tableCol23Partition123Statistics.get(partitionName).getColumnStatistics())
                .containsEntry("col2", intColumnStats(2))
                .containsEntry("col3", intColumnStats(3));
        assertThat(tableCol23Partition123Statistics.get(partition2Name).getColumnStatistics())
                .containsEntry("col2", intColumnStats(22))
                .containsEntry("col3", intColumnStats(23));
        assertThat(tableCol23Partition123Statistics.get(partition3Name).getColumnStatistics())
                .containsEntry("col2", intColumnStats(32))
                .containsEntry("col3", intColumnStats(33));
    }

    @Test
    public void testGetPartitionStatisticsWithoutMetadataCache()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        Table table = statsOnlyCacheMetastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        Partition partition = statsOnlyCacheMetastore.getPartition(table, TEST_PARTITION_VALUES1).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        assertThat(statsOnlyCacheMetastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        assertThat(statsOnlyCacheMetastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        assertThat(statsOnlyCacheMetastore.getPartitionStatisticsStats().getRequestCount()).isEqualTo(2);
        assertThat(statsOnlyCacheMetastore.getPartitionStatisticsStats().getHitRate()).isEqualTo(1.0 / 2);

        assertThat(statsOnlyCacheMetastore.getTableStats().getRequestCount()).isEqualTo(0);
        assertThat(statsOnlyCacheMetastore.getTableStats().getHitRate()).isEqualTo(1.0);

        assertThat(statsOnlyCacheMetastore.getPartitionStats().getRequestCount()).isEqualTo(0);
        assertThat(statsOnlyCacheMetastore.getPartitionStats().getHitRate()).isEqualTo(1.0);
    }

    @Test
    public void testInvalidatePartitionStatsWithLoadInProgress()
            throws Exception
    {
        CountDownLatch loadInProgress = new CountDownLatch(1);
        CountDownLatch invalidateDone = new CountDownLatch(1);
        MockThriftMetastoreClient mockClient = new MockThriftMetastoreClient()
        {
            @Override
            public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
                    throws TException
            {
                loadInProgress.countDown();
                Map<String, List<ColumnStatisticsObj>> result = super.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
                // get should wait for the invalidation to be done to return result
                try {
                    invalidateDone.await(10, SECONDS);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return result;
            }
        };
        CachingHiveMetastore metastore = createCachingHiveMetastore(new BridgingHiveMetastore(createThriftHiveMetastore(mockClient)), CACHE_TTL, true, true, executor);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();

        Partition partition = metastore.getPartition(table, TEST_PARTITION_VALUES1).orElseThrow();

        ExecutorService executorService = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder().setNameFormat("invalidation-%d").build());
        try {
            // invalidate thread
            Future<?> invalidateFuture = executorService.submit(
                    () -> {
                        try {
                            loadInProgress.await(10, SECONDS);
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        metastore.flushCache();
                        invalidateDone.countDown();
                    });

            // start get stats before the invalidation, it will wait until invalidation is done to finish
            assertThat(metastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
            assertThat(mockClient.getAccessCount()).isEqualTo(3);
            // get stats after invalidate
            assertThat(metastore.getPartitionStatistics(table, ImmutableList.of(partition))).isEqualTo(ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
            // the value was not cached
            assertThat(mockClient.getAccessCount()).isEqualTo(4);
            // make sure invalidateFuture is done
            invalidateFuture.get(1, SECONDS);
        }
        finally {
            executorService.shutdownNow();
        }
    }

    @Test
    public void testUpdatePartitionStatistics()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        HiveMetastoreClosure hiveMetastoreClosure = new HiveMetastoreClosure(metastore, TESTING_TYPE_MANAGER, false);

        Table table = hiveMetastoreClosure.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        hiveMetastoreClosure.updatePartitionStatistics(table.getDatabaseName(), table.getTableName(), TEST_PARTITION1, identity());
        assertThat(mockClient.getAccessCount()).isEqualTo(5);
    }

    @Test
    public void testInvalidGetPartitionsByNames()
    {
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(table, ImmutableList.of(BAD_PARTITION));
        assertThat(partitionsByNames.size()).isEqualTo(1);
        Optional<Partition> onlyElement = Iterables.getOnlyElement(partitionsByNames.values());
        assertThat(onlyElement.isPresent()).isFalse();
    }

    @Test
    public void testNoCacheExceptions()
    {
        // Throw exceptions on usage
        mockClient.setThrowException(true);
        try {
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        // Second try should hit the client again
        try {
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
    }

    @Test
    public void testNoCacheMissing()
    {
        CachingHiveMetastore metastore = createCachingHiveMetastore(new BridgingHiveMetastore(thriftHiveMetastore), CACHE_TTL, false, true, executor);

        mockClient.setReturnTable(false);
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        // First access
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        // Second access, second load
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        // Table get be accessed once it exists
        mockClient.setReturnTable(true);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        // Table existence is cached
        mockClient.setReturnTable(true);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        // Table is returned even if no longer exists
        mockClient.setReturnTable(false);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertThat(mockClient.getAccessCount()).isEqualTo(3);

        // After cache invalidation, table absence is apparent
        metastore.invalidateTable(TEST_DATABASE, TEST_TABLE);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
    }

    @Test
    public void testCachingHiveMetastoreCreationViaMemoize()
    {
        metastore = createPerTransactionCache(new BridgingHiveMetastore(createThriftHiveMetastore()), 1000);

        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        assertThat(metastore.getAllDatabases()).isEqualTo(ImmutableList.of(TEST_DATABASE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllDatabases()).isEqualTo(ImmutableList.of(TEST_DATABASE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getDatabaseNamesStats().getRequestCount()).isEqualTo(0);
    }

    @Test
    @Timeout(60)
    public void testLoadAfterInvalidate()
            throws Exception
    {
        testLoadAfterInvalidate(true);
        testLoadAfterInvalidate(false);
    }

    private void testLoadAfterInvalidate(boolean invalidateAll)
            throws Exception
    {
        // State
        CopyOnWriteArrayList<Column> tableColumns = new CopyOnWriteArrayList<>();
        ConcurrentMap<String, Partition> tablePartitionsByName = new ConcurrentHashMap<>();
        Map<String, String> tableParameters = new ConcurrentHashMap<>();
        tableParameters.put("frequent-changing-table-parameter", "parameter initial value");

        // Initialize data
        String databaseName = "my_database";
        String tableName = "my_table_name";

        tableColumns.add(new Column("value", toHiveType(VARCHAR), Optional.empty() /* comment */, Map.of()));
        tableColumns.add(new Column("pk", toHiveType(VARCHAR), Optional.empty() /* comment */, Map.of()));

        List<String> partitionNames = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            String partitionName = "pk=" + i;
            tablePartitionsByName.put(
                    partitionName,
                    Partition.builder()
                            .setDatabaseName(databaseName)
                            .setTableName(tableName)
                            .setColumns(ImmutableList.copyOf(tableColumns))
                            .setValues(List.of(Integer.toString(i)))
                            .withStorage(storage -> storage.setStorageFormat(fromHiveStorageFormat(TEXTFILE)))
                            .setParameters(Map.of("frequent-changing-partition-parameter", "parameter initial value"))
                            .build());
            partitionNames.add(partitionName);
        }

        // Mock metastore
        CountDownLatch getTableEnteredLatch = new CountDownLatch(1);
        CountDownLatch getTableReturnLatch = new CountDownLatch(1);
        CountDownLatch getTableFinishedLatch = new CountDownLatch(1);
        CountDownLatch getPartitionsByNamesEnteredLatch = new CountDownLatch(1);
        CountDownLatch getPartitionsByNamesReturnLatch = new CountDownLatch(1);
        CountDownLatch getPartitionsByNamesFinishedLatch = new CountDownLatch(1);

        HiveMetastore mockMetastore = new UnimplementedHiveMetastore()
        {
            @Override
            public Optional<Table> getTable(String databaseName, String tableName)
            {
                Optional<Table> table = Optional.of(Table.builder()
                        .setDatabaseName(databaseName)
                        .setTableName(tableName)
                        .setTableType(EXTERNAL_TABLE.name())
                        .setDataColumns(tableColumns)
                        .setParameters(ImmutableMap.copyOf(tableParameters))
                        // Required by 'Table', but not used by view translation.
                        .withStorage(storage -> storage.setStorageFormat(fromHiveStorageFormat(TEXTFILE)))
                        .setOwner(Optional.empty())
                        .build());

                getTableEnteredLatch.countDown(); // 1
                await(getTableReturnLatch, 10, SECONDS); // 2

                return table;
            }

            @Override
            public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
            {
                Map<String, Optional<Partition>> result = new HashMap<>();
                for (String partitionName : partitionNames) {
                    result.put(partitionName, Optional.ofNullable(tablePartitionsByName.get(partitionName)));
                }

                getPartitionsByNamesEnteredLatch.countDown(); // loader#1
                await(getPartitionsByNamesReturnLatch, 10, SECONDS); // loader#2

                return result;
            }
        };

        // Caching metastore
        metastore = createCachingHiveMetastore(mockMetastore, CACHE_TTL, true, true, executor);

        // The test. Main thread does modifications and verifies subsequent load sees them. Background thread loads the state into the cache.
        ExecutorService executor = Executors.newFixedThreadPool(1);
        try {
            Future<Void> future = executor.submit(() -> {
                try {
                    Table table;

                    table = metastore.getTable(databaseName, tableName).orElseThrow();
                    getTableFinishedLatch.countDown(); // 3

                    metastore.getPartitionsByNames(table, partitionNames);
                    getPartitionsByNamesFinishedLatch.countDown(); // 6

                    return null;
                }
                catch (Throwable e) {
                    log.error(e);
                    throw e;
                }
            });

            await(getTableEnteredLatch, 10, SECONDS); // 21
            tableParameters.put("frequent-changing-table-parameter", "main-thread-put-xyz");
            if (invalidateAll) {
                metastore.flushCache();
            }
            else {
                metastore.invalidateTable(databaseName, tableName);
            }
            getTableReturnLatch.countDown(); // 2
            await(getTableFinishedLatch, 10, SECONDS); // 3
            Table table = metastore.getTable(databaseName, tableName).orElseThrow();
            assertThat(table.getParameters())
                    .isEqualTo(Map.of("frequent-changing-table-parameter", "main-thread-put-xyz"));

            await(getPartitionsByNamesEnteredLatch, 10, SECONDS); // 4
            String partitionName = partitionNames.get(2);
            Map<String, String> newPartitionParameters = Map.of("frequent-changing-partition-parameter", "main-thread-put-alice");
            tablePartitionsByName.put(partitionName,
                    Partition.builder(tablePartitionsByName.get(partitionName))
                            .setParameters(newPartitionParameters)
                            .build());
            if (invalidateAll) {
                metastore.flushCache();
            }
            else {
                metastore.invalidateTable(databaseName, tableName);
            }
            getPartitionsByNamesReturnLatch.countDown(); // 5
            await(getPartitionsByNamesFinishedLatch, 10, SECONDS); // 6
            Map<String, Optional<Partition>> loadedPartitions = metastore.getPartitionsByNames(table, partitionNames);
            assertThat(loadedPartitions.get(partitionName))
                    .isNotNull()
                    .isPresent()
                    .hasValueSatisfying(partition -> assertThat(partition.getParameters()).isEqualTo(newPartitionParameters));

            // verify no failure in the background thread
            future.get(10, SECONDS);
        }
        finally {
            getTableEnteredLatch.countDown();
            getTableReturnLatch.countDown();
            getTableFinishedLatch.countDown();
            getPartitionsByNamesEnteredLatch.countDown();
            getPartitionsByNamesReturnLatch.countDown();
            getPartitionsByNamesFinishedLatch.countDown();

            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, SECONDS)).isTrue();
        }
    }

    @Test
    public void testDropTable()
    {
        // make sure the table and partition caches hit
        assertThat(mockClient.getAccessCount()).isEqualTo(0);
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(1);

        assertThat(metastore.getPartition(table, TEST_PARTITION_VALUES1)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getPartition(table, TEST_PARTITION_VALUES1)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(2);

        // the mock table is not really dropped, because it doesn't really exist in hive,
        // we just need to go through the cache invalidation logic in the try finally code block
        metastore.dropTable(TEST_DATABASE, TEST_TABLE, false);

        // pretend that the mock table is recreated, if the cache is invalidated, the access count should increment
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
        assertThat(metastore.getPartition(table, TEST_PARTITION_VALUES1)).isNotNull();
        assertThat(mockClient.getAccessCount()).isEqualTo(5);
    }

    @Test
    public void testAllDatabases()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(1); // should read it from cache

        metastore.dropDatabase(TEST_DATABASE, false);

        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(2);
        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(2); // should read it from cache

        metastore.createDatabase(
                Database.builder()
                        .setDatabaseName(TEST_DATABASE)
                        .setOwnerName(Optional.empty())
                        .setOwnerType(Optional.empty())
                        .build());

        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(3);
        assertThat(metastore.getAllDatabases()).containsExactly(TEST_DATABASE);
        assertThat(mockClient.getAccessCount()).isEqualTo(3); // should read it from cache
    }

    @Test
    public void testAllTables()
    {
        assertThat(mockClient.getAccessCount()).isEqualTo(0);

        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1);
        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(1); // should read it from cache

        metastore.dropTable(TEST_DATABASE, TEST_TABLE, false);
        assertThat(mockClient.getAccessCount()).isEqualTo(2); // dropTable check if the table exists

        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(3);
        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(3); // should read it from cache

        metastore.createTable(
                Table.builder()
                        .setDatabaseName(TEST_DATABASE)
                        .setTableName(TEST_TABLE)
                        .setOwner(Optional.empty())
                        .setTableType(VIRTUAL_VIEW.name())
                        .withStorage(storage -> storage.setStorageFormat(VIEW_STORAGE_FORMAT))
                        .build(),
                new PrincipalPrivileges(ImmutableMultimap.of(), ImmutableMultimap.of()));

        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(4);
        assertThat(metastore.getAllTables()).contains(ImmutableList.of(TEST_SCHEMA_TABLE));
        assertThat(mockClient.getAccessCount()).isEqualTo(4); // should read it from cache
    }

    private static HiveColumnStatistics intColumnStats(int nullsCount)
    {
        return createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.of(nullsCount), OptionalLong.empty());
    }

    private static void await(CountDownLatch latch, long timeout, TimeUnit unit)
    {
        try {
            boolean awaited = latch.await(timeout, unit);
            checkState(awaited, "wait timed out");
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException();
        }
    }

    private PartitionCachingAssertions assertThatCachingWithDisabledPartitionCache()
    {
        return new PartitionCachingAssertions(executor);
    }

    static class PartitionCachingAssertions
    {
        private final CachingHiveMetastore cachingHiveMetastore;
        private final MockThriftMetastoreClient thriftClient;
        private Consumer<CachingHiveMetastore> metastoreInteractions = hiveMetastore -> {};

        private PartitionCachingAssertions(Executor refreshExecutor)
        {
            thriftClient = new MockThriftMetastoreClient();
            cachingHiveMetastore = createCachingHiveMetastore(new BridgingHiveMetastore(createThriftHiveMetastore(thriftClient)), CACHE_TTL, true, false, refreshExecutor);
        }

        PartitionCachingAssertions whenExecuting(Consumer<CachingHiveMetastore> interactions)
        {
            this.metastoreInteractions = interactions;
            return this;
        }

        void usesCache()
        {
            for (int i = 0; i < 5; i++) {
                metastoreInteractions.accept(cachingHiveMetastore);
                assertThat(thriftClient.getAccessCount())
                        .describedAs("Metastore is expected to use cache, but it does not.")
                        .isEqualTo(1);
            }
        }

        void doesNotUseCache()
        {
            for (int i = 1; i < 5; i++) {
                metastoreInteractions.accept(cachingHiveMetastore);
                assertThat(thriftClient.getAccessCount())
                        .describedAs("Metastore is expected to not use cache, but it does.")
                        .isEqualTo(i);
            }
        }

        void omitsCacheForNumberOfOperations(int expectedCacheOmittingOperations)
        {
            //load caches
            metastoreInteractions.accept(cachingHiveMetastore);

            int startingAccessCount = thriftClient.getAccessCount();
            for (int i = 1; i < 5; i++) {
                metastoreInteractions.accept(cachingHiveMetastore);
                int currentAccessCount = thriftClient.getAccessCount();
                int timesCacheHasBeenOmitted = (currentAccessCount - startingAccessCount) / i;
                assertThat(timesCacheHasBeenOmitted)
                        .describedAs(format("Metastore is expected to not use cache %s times, but it does not use it %s times.",
                                expectedCacheOmittingOperations, timesCacheHasBeenOmitted))
                        .isEqualTo(expectedCacheOmittingOperations);
            }
        }
    }

    private static CachingHiveMetastore createCachingHiveMetastore(HiveMetastore hiveMetastore, Duration cacheTtl, boolean cacheMissing, boolean partitionCacheEnabled, Executor executor)
    {
        return CachingHiveMetastore.createCachingHiveMetastore(
                hiveMetastore,
                cacheTtl,
                CACHE_TTL,
                Optional.of(new Duration(1, TimeUnit.MINUTES)),
                executor,
                1000,
                CachingHiveMetastore.StatsRecording.ENABLED,
                cacheMissing,
                partitionCacheEnabled);
    }
}

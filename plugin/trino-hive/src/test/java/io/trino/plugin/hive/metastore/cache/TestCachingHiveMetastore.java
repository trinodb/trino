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
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.CachingHiveMetastoreBuilder;
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
import io.trino.testing.DataProviders;
import org.apache.thrift.TException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hive.HiveStorageFormat.TEXTFILE;
import static io.trino.plugin.hive.HiveType.HIVE_LONG;
import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static io.trino.plugin.hive.metastore.cache.TestCachingHiveMetastore.PartitionCachingAssertions.assertThatCachingWithDisabledPartitionCache;
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
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestCachingHiveMetastore
{
    private static final Logger log = Logger.get(TestCachingHiveMetastore.class);

    private static final PartitionStatistics TEST_STATS = PartitionStatistics.builder()
            .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(2398040535435L), OptionalLong.empty(), OptionalLong.empty()))
            .setColumnStatistics(ImmutableMap.of(TEST_COLUMN, createIntegerColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty())))
            .build();
    private static final SchemaTableName TEST_SCHEMA_TABLE = new SchemaTableName(TEST_DATABASE, TEST_TABLE);

    private MockThriftMetastoreClient mockClient;
    private ListeningExecutorService executor;
    private CachingHiveMetastoreBuilder metastoreBuilder;
    private CachingHiveMetastore metastore;
    private CachingHiveMetastore statsOnlyCacheMetastore;
    private ThriftMetastoreStats stats;

    @BeforeMethod
    public void setUp()
    {
        mockClient = new MockThriftMetastoreClient();
        ThriftMetastore thriftHiveMetastore = createThriftHiveMetastore();
        executor = listeningDecorator(newCachedThreadPool(daemonThreadsNamed(getClass().getSimpleName() + "-%s")));

        metastoreBuilder = CachingHiveMetastore.builder()
                .delegate(new BridgingHiveMetastore(thriftHiveMetastore))
                .executor(executor)
                .metadataCacheEnabled(true)
                .statsCacheEnabled(true)
                .cacheTtl(new Duration(5, TimeUnit.MINUTES))
                .refreshInterval(new Duration(1, TimeUnit.MINUTES))
                .maximumSize(1000)
                .cacheMissing(new CachingHiveMetastoreConfig().isCacheMissing())
                .partitionCacheEnabled(true);

        metastore = metastoreBuilder.build();
        statsOnlyCacheMetastore = CachingHiveMetastore.builder(metastoreBuilder)
                .metadataCacheEnabled(false)
                .statsCacheEnabled(true) // only cache stats
                .build();

        stats = ((ThriftHiveMetastore) thriftHiveMetastore).getStats();
    }

    @AfterClass(alwaysRun = true)
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
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getDatabaseNamesStats().getRequestCount(), 2);
        assertEquals(metastore.getDatabaseNamesStats().getHitRate(), 0.5);

        metastore.flushCache();

        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 2);
        assertEquals(metastore.getDatabaseNamesStats().getRequestCount(), 3);
        assertEquals(metastore.getDatabaseNamesStats().getHitRate(), 1.0 / 3);
    }

    @Test
    public void testGetAllTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getTableNamesStats().getRequestCount(), 2);
        assertEquals(metastore.getTableNamesStats().getHitRate(), 0.5);

        metastore.flushCache();

        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
        assertEquals(metastore.getTableNamesStats().getRequestCount(), 3);
        assertEquals(metastore.getTableNamesStats().getHitRate(), 1.0 / 3);
    }

    @Test
    public void testBatchGetAllTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllTables(), Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllTables(), Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllTables(TEST_DATABASE), ImmutableList.of(TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
        assertEquals(metastore.getAllTableNamesStats().getRequestCount(), 2);
        assertEquals(metastore.getAllTableNamesStats().getHitRate(), .5);

        metastore.flushCache();

        assertEquals(metastore.getAllTables(), Optional.of(ImmutableList.of(TEST_SCHEMA_TABLE)));
        assertEquals(mockClient.getAccessCount(), 3);
        assertEquals(metastore.getAllTableNamesStats().getRequestCount(), 3);
        assertEquals(metastore.getAllTableNamesStats().getHitRate(), 1. / 3);
    }

    @Test
    public void testInvalidDbGetAllTAbles()
    {
        assertTrue(metastore.getAllTables(BAD_DATABASE).isEmpty());
    }

    @Test
    public void testGetTable()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getTableStats().getRequestCount(), 2);
        assertEquals(metastore.getTableStats().getHitRate(), 0.5);

        metastore.flushCache();

        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 2);
        assertEquals(metastore.getTableStats().getRequestCount(), 3);
        assertEquals(metastore.getTableStats().getHitRate(), 1.0 / 3);
    }

    @Test
    public void testSetTableAuthorization()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertNotNull(metastore.getDatabase(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 2);
        metastore.setTableOwner(TEST_DATABASE, TEST_TABLE, new HivePrincipal(USER, "ignore"));
        assertEquals(mockClient.getAccessCount(), 3);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 4);
        // Assert that database cache has not been invalidated
        assertNotNull(metastore.getDatabase(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 4);
    }

    @Test
    public void testInvalidDbGetTable()
    {
        assertFalse(metastore.getTable(BAD_DATABASE, TEST_TABLE).isPresent());

        assertEquals(stats.getGetTable().getThriftExceptions().getTotalCount(), 0);
        assertEquals(stats.getGetTable().getTotalFailures().getTotalCount(), 0);
    }

    @Test
    public void testGetPartitionNames()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);
        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
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
    @Test(timeOut = 60_000, invocationCount = 20)
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
        assertTrue(metastore.getPartitionNamesByFilter(BAD_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).isEmpty());
    }

    @Test
    public void testGetPartitionNamesByParts()
    {
        ImmutableList<String> expectedPartitions = ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2, TEST_PARTITION3);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getPartitionFilterStats().getRequestCount(), 2);
        assertEquals(metastore.getPartitionFilterStats().getHitRate(), 0.5);

        metastore.flushCache();

        assertEquals(metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).orElseThrow(), expectedPartitions);
        assertEquals(mockClient.getAccessCount(), 2);
        assertEquals(metastore.getPartitionFilterStats().getRequestCount(), 3);
        assertEquals(metastore.getPartitionFilterStats().getHitRate(), 1.0 / 3);

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

        assertEquals(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount(), 0.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withNoFilter);
        assertEquals(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount(), 0.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withSingleValueFilter);
        assertEquals(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount(), 1.0);
        metastore.getPartitionNamesByFilter(TEST_DATABASE, TEST_TABLE, partitionColumnNames, withNoSingleValueFilter);
        assertEquals(stats.getGetPartitionNamesByParts().getTime().getAllTime().getCount(), 2.0);
    }

    @Test
    public void testInvalidGetPartitionNamesByParts()
    {
        assertFalse(metastore.getPartitionNamesByFilter(BAD_DATABASE, TEST_TABLE, PARTITION_COLUMN_NAMES, TupleDomain.all()).isPresent());
    }

    @Test
    public void testGetPartitionsByNames()
    {
        assertEquals(mockClient.getAccessCount(), 0);
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        // Select half of the available partitions and load them into the cache
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        assertEquals(mockClient.getAccessCount(), 2);

        // Now select all the partitions
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        // There should be one more access to fetch the remaining partition
        assertEquals(mockClient.getAccessCount(), 3);

        // Now if we fetch any or both of them, they should not hit the client
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1)).size(), 1);
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION2)).size(), 1);
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 3);

        metastore.flushCache();

        // Fetching both should only result in one batched access
        assertEquals(metastore.getPartitionsByNames(table, ImmutableList.of(TEST_PARTITION1, TEST_PARTITION2)).size(), 2);
        assertEquals(mockClient.getAccessCount(), 4);
    }

    @Test
    public void testListRoles()
    {
        assertEquals(mockClient.getAccessCount(), 0);

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(metastore.getRolesStats().getRequestCount(), 2);
        assertEquals(metastore.getRolesStats().getHitRate(), 0.5);

        metastore.flushCache();

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 2);

        metastore.createRole("role", "grantor");

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 3);

        metastore.dropRole("testrole");

        assertEquals(metastore.listRoles(), TEST_ROLES);
        assertEquals(mockClient.getAccessCount(), 4);

        assertEquals(metastore.getRolesStats().getRequestCount(), 5);
        assertEquals(metastore.getRolesStats().getHitRate(), 0.2);
    }

    @Test
    public void testGetTableStatistics()
    {
        assertEquals(mockClient.getAccessCount(), 0);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(metastore.getTableStatistics(table), TEST_STATS);
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(metastore.getTableStatistics(table), TEST_STATS);
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(metastore.getTableStatisticsStats().getRequestCount(), 2);
        assertEquals(metastore.getTableStatisticsStats().getHitRate(), 0.5);

        assertEquals(metastore.getTableStats().getRequestCount(), 1);
        assertEquals(metastore.getTableStats().getHitRate(), 0.0);

        // check empty column list does not trigger the call
        Table emptyColumnListTable = Table.builder(table).setDataColumns(ImmutableList.of()).build();
        assertThat(metastore.getTableStatistics(emptyColumnListTable).getBasicStatistics()).isEqualTo(TEST_STATS.getBasicStatistics());
        assertEquals(metastore.getTableStatisticsStats().getRequestCount(), 3);
        assertEquals(metastore.getTableStatisticsStats().getHitRate(), 2.0 / 3);

        mockClient.mockColumnStats(TEST_DATABASE, TEST_TABLE, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(1)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(2)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(3))));
        Table tableCol1 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty()))).build();
        assertThat(metastore.getTableStatistics(tableCol1).getColumnStatistics()).containsEntry("col1", intColumnStats(1));
        Table tableCol2 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty()))).build();
        assertThat(metastore.getTableStatistics(tableCol2).getColumnStatistics()).containsEntry("col2", intColumnStats(2));
        Table tableCol23 = Table.builder(table)
                .setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty()), new Column("col3", HIVE_LONG, Optional.empty())))
                .build();
        assertThat(metastore.getTableStatistics(tableCol23).getColumnStatistics())
                .containsEntry("col2", intColumnStats(2))
                .containsEntry("col3", intColumnStats(3));

        metastore.getTableStatistics(table); // ensure cached
        assertEquals(mockClient.getAccessCount(), 5);
        ColumnStatisticsData newStats = new ColumnStatisticsData();
        newStats.setLongStats(new LongColumnStatsData(327843, 4324));
        mockClient.mockColumnStats(TEST_DATABASE, TEST_TABLE, ImmutableMap.of(TEST_COLUMN, newStats));
        metastore.invalidateTable(TEST_DATABASE, TEST_TABLE);
        assertEquals(metastore.getTableStatistics(table), PartitionStatistics.builder()
                .setBasicStatistics(TEST_STATS.getBasicStatistics())
                .setColumnStatistics(ImmutableMap.of(TEST_COLUMN, createIntegerColumnStatistics(
                        OptionalLong.empty(),
                        OptionalLong.empty(),
                        OptionalLong.of(newStats.getLongStats().getNumNulls()),
                        OptionalLong.of(newStats.getLongStats().getNumDVs() - 1))))
                .build());
        assertEquals(mockClient.getAccessCount(), 6);
    }

    @Test
    public void testGetTableStatisticsWithoutMetadataCache()
    {
        assertEquals(mockClient.getAccessCount(), 0);

        Table table = statsOnlyCacheMetastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        assertEquals(statsOnlyCacheMetastore.getTableStatistics(table), TEST_STATS);
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(statsOnlyCacheMetastore.getTableStatistics(table), TEST_STATS);
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(statsOnlyCacheMetastore.getTableStatisticsStats().getRequestCount(), 2);
        assertEquals(statsOnlyCacheMetastore.getTableStatisticsStats().getHitRate(), 0.5);

        assertEquals(statsOnlyCacheMetastore.getTableStats().getRequestCount(), 0);
        assertEquals(statsOnlyCacheMetastore.getTableStats().getHitRate(), 1.0);
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
        CachingHiveMetastore metastore = createMetastore(mockClient);

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
            assertEquals(metastore.getTableStatistics(table), TEST_STATS);
            assertEquals(mockClient.getAccessCount(), 2);
            // get stats after invalidate
            assertEquals(metastore.getTableStatistics(table), TEST_STATS);
            // the value was not cached
            assertEquals(mockClient.getAccessCount(), 3);
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
        assertEquals(mockClient.getAccessCount(), 0);

        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        Partition partition = metastore.getPartition(table, TEST_PARTITION_VALUES1).orElseThrow();
        String partitionName = makePartitionName(table, partition);
        Partition partition2 = metastore.getPartition(table, TEST_PARTITION_VALUES2).orElseThrow();
        String partition2Name = makePartitionName(table, partition2);
        Partition partition3 = metastore.getPartition(table, TEST_PARTITION_VALUES3).orElseThrow();
        String partition3Name = makePartitionName(table, partition3);
        assertEquals(mockClient.getAccessCount(), 4);

        assertEquals(metastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertEquals(mockClient.getAccessCount(), 5);

        assertEquals(metastore.getPartitionStatisticsStats().getRequestCount(), 1);
        assertEquals(metastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertEquals(mockClient.getAccessCount(), 5);

        assertEquals(metastore.getPartitionStatisticsStats().getRequestCount(), 2);
        assertEquals(metastore.getPartitionStatisticsStats().getHitRate(), 1.0 / 2);

        assertEquals(metastore.getTableStats().getRequestCount(), 1);
        assertEquals(metastore.getTableStats().getHitRate(), 0.0);

        assertEquals(metastore.getPartitionStats().getRequestCount(), 3);
        assertEquals(metastore.getPartitionStats().getHitRate(), 0.0);

        // check empty column list does not trigger the call
        Table emptyColumnListTable = Table.builder(table).setDataColumns(ImmutableList.of()).build();
        Map<String, PartitionStatistics> partitionStatistics = metastore.getPartitionStatistics(emptyColumnListTable, ImmutableList.of(partition));
        assertThat(partitionStatistics).containsOnlyKeys(TEST_PARTITION1);
        assertThat(partitionStatistics.get(TEST_PARTITION1).getBasicStatistics()).isEqualTo(TEST_STATS.getBasicStatistics());
        assertEquals(metastore.getPartitionStatisticsStats().getRequestCount(), 3);
        assertEquals(metastore.getPartitionStatisticsStats().getHitRate(), 2.0 / 3);

        mockClient.mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION1, ImmutableMap.of(
                "col1", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(1)),
                "col2", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(2)),
                "col3", ColumnStatisticsData.longStats(new LongColumnStatsData().setNumNulls(3))));

        Table tableCol1 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col1", HIVE_LONG, Optional.empty()))).build();
        Map<String, PartitionStatistics> tableCol1PartitionStatistics = metastore.getPartitionStatistics(tableCol1, ImmutableList.of(partition));
        assertThat(tableCol1PartitionStatistics).containsOnlyKeys(partitionName);
        assertThat(tableCol1PartitionStatistics.get(partitionName).getColumnStatistics()).containsEntry("col1", intColumnStats(1));
        Table tableCol2 = Table.builder(table).setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty()))).build();
        Map<String, PartitionStatistics> tableCol2PartitionStatistics = metastore.getPartitionStatistics(tableCol2, ImmutableList.of(partition));
        assertThat(tableCol2PartitionStatistics).containsOnlyKeys(partitionName);
        assertThat(tableCol2PartitionStatistics.get(partitionName).getColumnStatistics()).containsEntry("col2", intColumnStats(2));
        Table tableCol23 = Table.builder(table)
                .setDataColumns(ImmutableList.of(new Column("col2", HIVE_LONG, Optional.empty()), new Column("col3", HIVE_LONG, Optional.empty())))
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
        assertEquals(mockClient.getAccessCount(), 0);

        Table table = statsOnlyCacheMetastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        Partition partition = statsOnlyCacheMetastore.getPartition(table, TEST_PARTITION_VALUES1).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 2);

        assertEquals(statsOnlyCacheMetastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertEquals(mockClient.getAccessCount(), 3);

        assertEquals(statsOnlyCacheMetastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
        assertEquals(mockClient.getAccessCount(), 3);

        assertEquals(statsOnlyCacheMetastore.getPartitionStatisticsStats().getRequestCount(), 2);
        assertEquals(statsOnlyCacheMetastore.getPartitionStatisticsStats().getHitRate(), 1.0 / 2);

        assertEquals(statsOnlyCacheMetastore.getTableStats().getRequestCount(), 0);
        assertEquals(statsOnlyCacheMetastore.getTableStats().getHitRate(), 1.0);

        assertEquals(statsOnlyCacheMetastore.getPartitionStats().getRequestCount(), 0);
        assertEquals(statsOnlyCacheMetastore.getPartitionStats().getHitRate(), 1.0);
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
        CachingHiveMetastore metastore = createMetastore(mockClient);

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
            assertEquals(metastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
            assertEquals(mockClient.getAccessCount(), 3);
            // get stats after invalidate
            assertEquals(metastore.getPartitionStatistics(table, ImmutableList.of(partition)), ImmutableMap.of(TEST_PARTITION1, TEST_STATS));
            // the value was not cached
            assertEquals(mockClient.getAccessCount(), 4);
            // make sure invalidateFuture is done
            invalidateFuture.get(1, SECONDS);
        }
        finally {
            executorService.shutdownNow();
        }
    }

    private CachingHiveMetastore createMetastore(MockThriftMetastoreClient mockClient)
    {
        return CachingHiveMetastore.builder()
                .delegate(new BridgingHiveMetastore(createThriftHiveMetastore(mockClient)))
                .executor(executor)
                .metadataCacheEnabled(true)
                .statsCacheEnabled(true)
                .cacheTtl(new Duration(5, TimeUnit.MINUTES))
                .refreshInterval(new Duration(1, TimeUnit.MINUTES))
                .maximumSize(1000)
                .cacheMissing(new CachingHiveMetastoreConfig().isCacheMissing())
                .partitionCacheEnabled(true)
                .build();
    }

    @Test
    public void testUpdatePartitionStatistics()
    {
        assertEquals(mockClient.getAccessCount(), 0);

        HiveMetastoreClosure hiveMetastoreClosure = new HiveMetastoreClosure(metastore);

        Table table = hiveMetastoreClosure.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);

        hiveMetastoreClosure.updatePartitionStatistics(table.getDatabaseName(), table.getTableName(), TEST_PARTITION1, identity());
        assertEquals(mockClient.getAccessCount(), 5);
    }

    @Test
    public void testInvalidGetPartitionsByNames()
    {
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        Map<String, Optional<Partition>> partitionsByNames = metastore.getPartitionsByNames(table, ImmutableList.of(BAD_PARTITION));
        assertEquals(partitionsByNames.size(), 1);
        Optional<Partition> onlyElement = Iterables.getOnlyElement(partitionsByNames.values());
        assertFalse(onlyElement.isPresent());
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
        assertEquals(mockClient.getAccessCount(), 1);

        // Second try should hit the client again
        try {
            metastore.getAllDatabases();
        }
        catch (RuntimeException ignored) {
        }
        assertEquals(mockClient.getAccessCount(), 2);
    }

    @Test
    public void testNoCacheMissing()
    {
        CachingHiveMetastore metastore = CachingHiveMetastore.builder(metastoreBuilder)
                .cacheMissing(false)
                .build();

        mockClient.setReturnTable(false);
        assertEquals(mockClient.getAccessCount(), 0);

        // First access
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertEquals(mockClient.getAccessCount(), 1);

        // Second access, second load
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertEquals(mockClient.getAccessCount(), 2);

        // Table get be accessed once it exists
        mockClient.setReturnTable(true);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertEquals(mockClient.getAccessCount(), 3);

        // Table existence is cached
        mockClient.setReturnTable(true);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertEquals(mockClient.getAccessCount(), 3);

        // Table is returned even if no longer exists
        mockClient.setReturnTable(false);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isPresent();
        assertEquals(mockClient.getAccessCount(), 3);

        // After cache invalidation, table absence is apparent
        metastore.invalidateTable(TEST_DATABASE, TEST_TABLE);
        assertThat(metastore.getTable(TEST_DATABASE, TEST_TABLE)).isEmpty();
        assertEquals(mockClient.getAccessCount(), 4);
    }

    @Test
    public void testCachingHiveMetastoreCreationWithTtlOnly()
    {
        CachingHiveMetastoreConfig config = new CachingHiveMetastoreConfig();
        config.setMetastoreCacheTtl(new Duration(10, TimeUnit.MILLISECONDS));

        CachingHiveMetastore metastore = createMetastoreWithDirectExecutor(config);

        assertThat(metastore).isNotNull();
    }

    @Test
    public void testCachingHiveMetastoreCreationViaMemoize()
    {
        ThriftMetastore thriftHiveMetastore = createThriftHiveMetastore();
        metastore = memoizeMetastore(
                new BridgingHiveMetastore(thriftHiveMetastore),
                1000);

        assertEquals(mockClient.getAccessCount(), 0);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getAllDatabases(), ImmutableList.of(TEST_DATABASE));
        assertEquals(mockClient.getAccessCount(), 1);
        assertEquals(metastore.getDatabaseNamesStats().getRequestCount(), 0);
    }

    @Test(timeOut = 60_000, dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testLoadAfterInvalidate(boolean invalidateAll)
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

        tableColumns.add(new Column("value", toHiveType(VARCHAR), Optional.empty() /* comment */));
        tableColumns.add(new Column("pk", toHiveType(VARCHAR), Optional.empty() /* comment */));

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
        metastore = CachingHiveMetastore.builder()
                .delegate(mockMetastore)
                .executor(executor)
                .metadataCacheEnabled(true)
                .statsCacheEnabled(true)
                .cacheTtl(new Duration(5, TimeUnit.MINUTES))
                .refreshInterval(new Duration(1, TimeUnit.MINUTES))
                .maximumSize(1000)
                .cacheMissing(new CachingHiveMetastoreConfig().isCacheMissing())
                .partitionCacheEnabled(true)
                .build();

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
            assertTrue(executor.awaitTermination(10, SECONDS));
        }
    }

    @Test
    public void testDropTable()
    {
        // make sure the table and partition caches hit
        assertEquals(mockClient.getAccessCount(), 0);
        Table table = metastore.getTable(TEST_DATABASE, TEST_TABLE).orElseThrow();
        assertEquals(mockClient.getAccessCount(), 1);
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 1);

        assertNotNull(metastore.getPartition(table, TEST_PARTITION_VALUES1));
        assertEquals(mockClient.getAccessCount(), 2);
        assertNotNull(metastore.getPartition(table, TEST_PARTITION_VALUES1));
        assertEquals(mockClient.getAccessCount(), 2);

        // the mock table is not really dropped, because it doesn't really exist in hive,
        // we just need to go through the cache invalidation logic in the try finally code block
        metastore.dropTable(TEST_DATABASE, TEST_TABLE, false);

        // pretend that the mock table is recreated, if the cache is invalidated, the access count should increment
        assertNotNull(metastore.getTable(TEST_DATABASE, TEST_TABLE));
        assertEquals(mockClient.getAccessCount(), 4);
        assertNotNull(metastore.getPartition(table, TEST_PARTITION_VALUES1));
        assertEquals(mockClient.getAccessCount(), 5);
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

    static class PartitionCachingAssertions
    {
        private final CachingHiveMetastore cachingHiveMetastore;
        private final MockThriftMetastoreClient thriftClient;
        private Consumer<CachingHiveMetastore> metastoreInteractions = hiveMetastore -> {};

        static PartitionCachingAssertions assertThatCachingWithDisabledPartitionCache()
        {
            return new PartitionCachingAssertions();
        }

        private PartitionCachingAssertions()
        {
            thriftClient = new MockThriftMetastoreClient();
            cachingHiveMetastore = CachingHiveMetastore.builder()
                    .delegate(new BridgingHiveMetastore(createThriftHiveMetastore(thriftClient)))
                    .executor(listeningDecorator(newCachedThreadPool(daemonThreadsNamed("test-%s"))))
                    .metadataCacheEnabled(true)
                    .statsCacheEnabled(true)
                    .cacheTtl(new Duration(5, TimeUnit.MINUTES))
                    .refreshInterval(new Duration(1, TimeUnit.MINUTES))
                    .maximumSize(1000)
                    .cacheMissing(true)
                    .partitionCacheEnabled(false)
                    .build();
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
                assertEquals(thriftClient.getAccessCount(), 1, "Metastore is expected to use cache, but it does not.");
            }
        }

        void doesNotUseCache()
        {
            for (int i = 1; i < 5; i++) {
                metastoreInteractions.accept(cachingHiveMetastore);
                assertEquals(thriftClient.getAccessCount(), i, "Metastore is expected to not use cache, but it does.");
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
                assertEquals(timesCacheHasBeenOmitted, expectedCacheOmittingOperations, format("Metastore is expected to not use cache %s times, but it does not use it %s times.",
                        expectedCacheOmittingOperations, timesCacheHasBeenOmitted));
            }
        }
    }

    private CachingHiveMetastore createMetastoreWithDirectExecutor(CachingHiveMetastoreConfig config)
    {
        return CachingHiveMetastore.builder()
                .delegate(new BridgingHiveMetastore(createThriftHiveMetastore()))
                .executor(directExecutor())
                .metadataCacheEnabled(true)
                .statsCacheEnabled(true)
                .cacheTtl(config.getMetastoreCacheTtl())
                .refreshInterval(config.getMetastoreRefreshInterval())
                .maximumSize(config.getMetastoreCacheMaximumSize())
                .cacheMissing(config.isCacheMissing())
                .partitionCacheEnabled(config.isPartitionCacheEnabled())
                .build();
    }
}

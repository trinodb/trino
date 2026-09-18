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
package io.trino.plugin.hive.metastore.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import io.trino.hive.thrift.metastore.ColumnStatisticsData;
import io.trino.hive.thrift.metastore.ColumnStatisticsObj;
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.Partition;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.Table;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.PartitionStatistics;
import io.trino.spi.TrinoException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.HiveBasicStatistics.createEmptyStatistics;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_COLUMN;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_DATABASE;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION1;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_PARTITION2;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_TABLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestThriftHiveMetastore
{
    private static final List<String> COLUMNS = ImmutableList.of("a", "b", "c");
    private static final String PARTITION_COLUMN = "key";
    private static final PartitionStatistics EMPTY_STATISTICS = new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of());
    private static final PartitionStatistics COLUMN_STATISTICS = new PartitionStatistics(
            createEmptyStatistics(),
            ImmutableMap.of(TEST_COLUMN, HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(10), OptionalLong.of(0), OptionalLong.of(5))));

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void tearDown()
            throws Exception
    {
        for (AutoCloseable resource : resources) {
            resource.close();
        }
    }

    @Test
    void testRetryResumesFromFailedColumn()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of("b", new TTransportException("connection reset")));
        clearColumnStatistics(client);
        assertThat(client.deleteAttempts).containsExactly("a", "b", "b", "c");
    }

    @Test
    void testMetastoreErrorStopsRetry()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of("b", new MetaException("boom")));
        assertThatThrownBy(() -> clearColumnStatistics(client)).isInstanceOf(TrinoException.class);
        assertThat(client.deleteAttempts).containsExactly("a", "b");
    }

    @Test
    void testMissingColumnStatisticsIsSkipped()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of("b", new NoSuchObjectException()));
        clearColumnStatistics(client);
        assertThat(client.deleteAttempts).containsExactly("a", "b", "c");
    }

    @Test
    void testUpdatePartitionStatisticsUsesBulkRequests()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of());
        createMetastore(client).updatePartitionStatistics(partitionedTable(), OVERWRITE_ALL, ImmutableMap.of(TEST_PARTITION1, COLUMN_STATISTICS, TEST_PARTITION2, COLUMN_STATISTICS));
        assertThat(client.partitionCalls).containsExactly(
                "getPartitionsByNames[key=testpartition1, key=testpartition2]",
                "close",
                "getPartitionColumnStatistics[key=testpartition1, key=testpartition2]",
                "close",
                "alterPartitions[key=testpartition1, key=testpartition2]",
                "close",
                "setPartitionsColumnStatistics[key=testpartition1, key=testpartition2]",
                "close");
    }

    @Test
    void testUpdatePartitionStatisticsDeletesRemovedColumnStatisticsOnOneConnection()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of());
        createMetastore(client).updatePartitionStatistics(partitionedTable(), OVERWRITE_ALL, ImmutableMap.of(TEST_PARTITION1, EMPTY_STATISTICS, TEST_PARTITION2, EMPTY_STATISTICS));
        assertThat(client.partitionCalls).containsExactly(
                "getPartitionsByNames[key=testpartition1, key=testpartition2]",
                "close",
                "getPartitionColumnStatistics[key=testpartition1, key=testpartition2]",
                "close",
                "alterPartitions[key=testpartition1, key=testpartition2]",
                "close",
                "deletePartitionColumnStatistics key=testpartition1.column",
                "deletePartitionColumnStatistics key=testpartition2.column",
                "close");
    }

    @Test
    void testUpdatePartitionStatisticsFailsForMissingPartition()
    {
        RecordingClient client = new RecordingClient(ImmutableMap.of());
        assertThatThrownBy(() -> createMetastore(client).updatePartitionStatistics(partitionedTable(), OVERWRITE_ALL, ImmutableMap.of(TEST_PARTITION1, EMPTY_STATISTICS, "key=missing", EMPTY_STATISTICS)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("No partition found for names: key=testpartition1, key=missing");
        assertThat(client.partitionCalls).containsExactly("getPartitionsByNames[key=testpartition1, key=missing]", "close");
    }

    private void clearColumnStatistics(ThriftMetastoreClient client)
    {
        createMetastore(client).updateTableStatistics(TEST_DATABASE, TEST_TABLE, OptionalLong.empty(), OVERWRITE_ALL, EMPTY_STATISTICS);
    }

    private ThriftMetastore createMetastore(ThriftMetastoreClient client)
    {
        return testingThriftHiveMetastoreBuilder()
                .metastoreClient(client)
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        .setMinBackoffDelay(new Duration(1, MILLISECONDS))
                        .setMaxBackoffDelay(new Duration(1, MILLISECONDS)))
                .build(resources::add);
    }

    private static Table partitionedTable()
    {
        StorageDescriptor storage = new StorageDescriptor(ImmutableList.of(new FieldSchema(TEST_COLUMN, "bigint", "")), "", null, null, false, 0, new SerDeInfo(TEST_TABLE, null, ImmutableMap.of()), null, null, ImmutableMap.of());
        return new Table(TEST_TABLE, TEST_DATABASE, "", 0, 0, 0, storage, ImmutableList.of(new FieldSchema(PARTITION_COLUMN, "string", "")), ImmutableMap.of(), "", "", MANAGED_TABLE.name());
    }

    private static final class RecordingClient
            extends MockThriftMetastoreClient
    {
        private final Map<String, TException> pendingFailures;
        private final List<String> deleteAttempts = new ArrayList<>();
        private final List<String> partitionCalls = new ArrayList<>();

        RecordingClient(Map<String, TException> failures)
        {
            this.pendingFailures = new HashMap<>(failures);
            Map<String, ColumnStatisticsData> columnStatistics = new HashMap<>();
            for (String column : COLUMNS) {
                ColumnStatisticsData data = new ColumnStatisticsData();
                data.setLongStats(new LongColumnStatsData());
                columnStatistics.put(column, data);
            }
            mockColumnStats(TEST_DATABASE, TEST_TABLE, columnStatistics);
            ColumnStatisticsData partitionColumnStatistics = new ColumnStatisticsData();
            partitionColumnStatistics.setLongStats(new LongColumnStatsData());
            mockPartitionColumnStats(TEST_DATABASE, TEST_TABLE, TEST_PARTITION2, ImmutableMap.of(TEST_COLUMN, partitionColumnStatistics));
        }

        @Override
        public void close()
        {
            partitionCalls.add("close");
        }

        @Override
        public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> names)
                throws TException
        {
            partitionCalls.add("getPartitionsByNames" + names);
            return super.getPartitionsByNames(databaseName, tableName, names);
        }

        @Override
        public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String databaseName, String tableName, List<String> partitionNames, List<String> columnNames)
                throws TException
        {
            partitionCalls.add("getPartitionColumnStatistics" + partitionNames);
            return super.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
        }

        @Override
        public void alterPartitions(String databaseName, String tableName, List<Partition> partitions)
        {
            partitionCalls.add("alterPartitions" + partitions.stream()
                    .map(partition -> makePartName(ImmutableList.of(PARTITION_COLUMN), partition.getValues()))
                    .collect(toImmutableList()));
        }

        @Override
        public void setPartitionsColumnStatistics(String databaseName, String tableName, Map<String, List<ColumnStatisticsObj>> partitionStatistics)
        {
            partitionCalls.add("setPartitionsColumnStatistics" + ImmutableList.copyOf(partitionStatistics.keySet()));
        }

        @Override
        public void deletePartitionColumnStatistics(String databaseName, String tableName, String partitionName, String columnName)
        {
            partitionCalls.add("deletePartitionColumnStatistics " + partitionName + "." + columnName);
        }

        @Override
        public Table getTable(String dbName, String tableName)
        {
            List<FieldSchema> fields = COLUMNS.stream()
                    .map(column -> new FieldSchema(column, "bigint", ""))
                    .collect(toImmutableList());
            StorageDescriptor storage = new StorageDescriptor(fields, "", null, null, false, 0, new SerDeInfo(TEST_TABLE, null, ImmutableMap.of()), null, null, ImmutableMap.of());
            return new Table(TEST_TABLE, TEST_DATABASE, "", 0, 0, 0, storage, ImmutableList.of(), ImmutableMap.of(), "", "", MANAGED_TABLE.name());
        }

        @Override
        public void deleteTableColumnStatistics(String databaseName, String tableName, String columnName)
                throws TException
        {
            deleteAttempts.add(columnName);
            TException failure = pendingFailures.remove(columnName);
            if (failure != null) {
                throw failure;
            }
        }
    }
}

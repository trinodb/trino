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
import io.trino.hive.thrift.metastore.FieldSchema;
import io.trino.hive.thrift.metastore.LongColumnStatsData;
import io.trino.hive.thrift.metastore.MetaException;
import io.trino.hive.thrift.metastore.NoSuchObjectException;
import io.trino.hive.thrift.metastore.SerDeInfo;
import io.trino.hive.thrift.metastore.StorageDescriptor;
import io.trino.hive.thrift.metastore.Table;
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
import static io.trino.metastore.StatisticsUpdateMode.OVERWRITE_ALL;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TestingThriftHiveMetastoreBuilder.testingThriftHiveMetastoreBuilder;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_DATABASE;
import static io.trino.plugin.hive.metastore.thrift.MockThriftMetastoreClient.TEST_TABLE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestThriftHiveMetastore
{
    private static final List<String> COLUMNS = ImmutableList.of("a", "b", "c");

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

    private void clearColumnStatistics(ThriftMetastoreClient client)
    {
        ThriftMetastore metastore = testingThriftHiveMetastoreBuilder()
                .metastoreClient(client)
                .thriftMetastoreConfig(new ThriftMetastoreConfig()
                        .setMinBackoffDelay(new Duration(1, MILLISECONDS))
                        .setMaxBackoffDelay(new Duration(1, MILLISECONDS)))
                .build(resources::add);
        metastore.updateTableStatistics(TEST_DATABASE, TEST_TABLE, OptionalLong.empty(), OVERWRITE_ALL, new PartitionStatistics(createEmptyStatistics(), ImmutableMap.of()));
    }

    private static final class RecordingClient
            extends MockThriftMetastoreClient
    {
        private final Map<String, TException> pendingFailures;
        private final List<String> deleteAttempts = new ArrayList<>();

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

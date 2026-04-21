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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.cassandra.CassandraTestingUtils.CASSANDRA_TYPE_MANAGER;
import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
final class TestCassandraSplitManager
{
    private static final String KEYSPACE = "test_cassandra_split_manager_keyspace";

    private CassandraServer server;
    private CassandraSession session;

    @BeforeAll
    void setUp()
            throws Exception
    {
        server = new CassandraServer();
        session = server.getSession();
        createKeyspace(session, KEYSPACE);
    }

    @AfterAll
    void tearDown()
            throws Exception
    {
        closeAll(server, session);
        server = null;
        session = null;
    }

    @Test
    void testGetSplitsWithSinglePartitionKeyColumn()
            throws Exception
    {
        String tableName = "single_partition_key_column_table";
        int partitionCount = 3;

        session.execute(format(
                """
                CREATE TABLE %s.%s (
                      partition_key int,
                      clustering_key text,
                      PRIMARY KEY(partition_key, clustering_key))
                """,
                KEYSPACE,
                tableName));

        CassandraColumnHandle columnHandle = new CassandraColumnHandle("partition_key", 0, CassandraTypes.INT, true, false, false, false);
        ImmutableList.Builder<CassandraPartition> partitions = ImmutableList.builderWithExpectedSize(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            TupleDomain<ColumnHandle> tupleDomain = TupleDomain.fromFixedValues(Map.of(columnHandle, NullableValue.of(CassandraTypes.INT.trinoType(), (long) i)));
            partitions.add(new CassandraPartition(new byte[] {0, 0, 0, (byte) i}, format("\"partition_key\" = %d", i), tupleDomain, false));
            session.execute(format("INSERT INTO %s.%s (partition_key, clustering_key) VALUES (%d, '%d')", KEYSPACE, tableName, i, i));
        }

        CassandraPartitionManager partitionManager = new CassandraPartitionManager(session, CASSANDRA_TYPE_MANAGER);
        CassandraClientConfig config = new CassandraClientConfig().setPartitionSizeForBatchSelect(partitionCount - 1);
        CassandraSplitManager splitManager = new CassandraSplitManager(config, session, null, partitionManager, CASSANDRA_TYPE_MANAGER);

        CassandraTableHandle tableHandle = new CassandraTableHandle(
                new CassandraNamedRelationHandle(KEYSPACE, tableName, Optional.of(partitions.build()), ""));
        try (ConnectorSplitSource splitSource = splitManager.getSplits(null, null, tableHandle, null, null)) {
            List<ConnectorSplit> splits = splitSource.getNextBatch(100).get().getSplits();
            assertThat(splits).hasSize(2);
            assertThat(((CassandraSplit) splits.get(0)).partitionId()).isEqualTo("\"partition_key\" in (0,1)");
            assertThat(((CassandraSplit) splits.get(1)).partitionId()).isEqualTo("\"partition_key\" in (2)");
        }

        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }
}

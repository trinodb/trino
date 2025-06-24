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

import io.trino.plugin.cassandra.CassandraTokenSplitManager.TokenSplit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.cassandra.CassandraTestingUtils.createKeyspace;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestCassandraTokenSplitManager
{
    private static final int SPLIT_SIZE = 100;
    private static final String KEYSPACE = "test_cassandra_token_split_manager_keyspace";
    private static final int PARTITION_COUNT = 1000;

    private TestingCassandraServer server;
    private CassandraSession session;
    private CassandraTokenSplitManager splitManager;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        server = new TestingCassandraServer();
        session = server.getSession();
        createKeyspace(session, KEYSPACE);
        splitManager = new CassandraTokenSplitManager(session, SPLIT_SIZE, Optional.empty());
    }

    @AfterAll
    public void tearDown()
    {
        server.close();
        server = null;
        session.close();
        session = null;
    }

    @Test
    public void testPartitionCountOverride()
            throws Exception
    {
        String tableName = "partition_count_override_table";
        session.execute(format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, tableName));
        server.refreshSizeEstimates(KEYSPACE, tableName);

        CassandraTokenSplitManager onlyConfigSplitsPerNode = new CassandraTokenSplitManager(session, SPLIT_SIZE, Optional.of(12_345L));
        assertThat(onlyConfigSplitsPerNode.getTotalPartitionsCount(KEYSPACE, tableName, Optional.empty())).isEqualTo(12_345L);

        CassandraTokenSplitManager onlySessionSplitsPerNode = new CassandraTokenSplitManager(session, SPLIT_SIZE, Optional.empty());
        assertThat(onlySessionSplitsPerNode.getTotalPartitionsCount(KEYSPACE, tableName, Optional.of(67_890L))).isEqualTo(67_890L);

        CassandraTokenSplitManager sessionOverrideConfig = new CassandraTokenSplitManager(session, SPLIT_SIZE, Optional.of(12_345L));
        assertThat(sessionOverrideConfig.getTotalPartitionsCount(KEYSPACE, tableName, Optional.of(67_890L))).isEqualTo(67_890L);

        CassandraTokenSplitManager defaultSplitManager = new CassandraTokenSplitManager(session, SPLIT_SIZE, Optional.empty());
        assertThat(defaultSplitManager.getTotalPartitionsCount(KEYSPACE, tableName, Optional.empty())).isEqualTo(0);
    }

    @Test
    public void testEmptyTable()
            throws Exception
    {
        String tableName = "empty_table";
        session.execute(format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, tableName));
        server.refreshSizeEstimates(KEYSPACE, tableName);
        List<TokenSplit> splits = splitManager.getSplits(KEYSPACE, tableName, Optional.empty());
        // even for the empty table at least one split must be produced, in case the statistics are inaccurate
        assertThat(splits).hasSize(1);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }

    @Test
    public void testNonEmptyTable()
            throws Exception
    {
        String tableName = "non_empty_table";
        session.execute(format("CREATE TABLE %s.%s (key text PRIMARY KEY)", KEYSPACE, tableName));
        for (int i = 0; i < PARTITION_COUNT; i++) {
            session.execute(format("INSERT INTO %s.%s (key) VALUES ('%s')", KEYSPACE, tableName, "value" + i));
        }
        server.refreshSizeEstimates(KEYSPACE, tableName);
        List<TokenSplit> splits = splitManager.getSplits(KEYSPACE, tableName, Optional.empty());
        int expectedTokenSplitSize = PARTITION_COUNT / SPLIT_SIZE;
        // Use hasSizeBetween because Cassandra server may overestimate the size
        assertThat(splits).hasSizeBetween(expectedTokenSplitSize, expectedTokenSplitSize + 1);
        session.execute(format("DROP TABLE %s.%s", KEYSPACE, tableName));
    }
}

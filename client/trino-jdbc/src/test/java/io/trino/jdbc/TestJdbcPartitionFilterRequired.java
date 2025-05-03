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
package io.trino.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logging;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.plugin.hive.HivePlugin;
import io.trino.server.testing.TestingTrinoServer;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SystemTable;
import io.trino.spi.connector.SystemTable.Distribution;
import io.trino.spi.predicate.TupleDomain;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Closeables.closeAll;
import static io.trino.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.trino.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJdbcPartitionFilterRequired
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed(getClass().getName()));

    private TestingTrinoServer server;

    @BeforeAll
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        Module systemTables = binder -> newSetBinder(binder, SystemTable.class)
                .addBinding().to(ExtraCredentialsSystemTable.class).in(Scopes.SINGLETON);
        server = TestingTrinoServer.builder()
                .setAdditionalModule(systemTables)
                .build();
        server.installPlugin(new HivePlugin());
        server.createCatalog("hive", "hive", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getBaseDataDir().resolve("hive").toAbsolutePath().toString())
                .put("hive.security", "sql-standard")
                .put("fs.hadoop.enabled", "true")
                .put("hive.query-partition-filter-required", "true")
                .buildOrThrow());
        server.installPlugin(new BlackHolePlugin());

        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("SET ROLE admin IN hive");
            statement.execute("CREATE SCHEMA default");

            statement.execute("CREATE TABLE test_required_partition_filter(id integer, a varchar, b varchar, ds varchar) WITH (partitioned_by = ARRAY['ds'])");
            statement.execute("INSERT INTO test_required_partition_filter(id, a, ds) VALUES (1, 'a', '1')");
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        closeAll(
                server,
                executor::shutdownNow);
        server = null;
    }

    @Test
    public void testFilterRequiredPartitions()
            throws SQLException
    {
        String sql = "SELECT id FROM test_required_partition_filter WHERE a = '1' ";
        assertThatThrownBy(() -> {
            try (Connection connection = createConnection()) {
                Statement statement = connection.createStatement();
                statement.execute(sql);
                ResultSet resultSet = statement.getResultSet();
                assertThat(resultSet.next()).isTrue();
            }
        }).isInstanceOf(SQLException.class);
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection("");
    }

    private Connection createConnection(String extra)
            throws SQLException
    {
        String url = format("jdbc:trino://%s/hive/default?%s", server.getAddress(), extra);
        return DriverManager.getConnection(url, "admin", null);
    }

    private static class ExtraCredentialsSystemTable
            implements SystemTable
    {
        private static final SchemaTableName NAME = new SchemaTableName("test", "extra_credentials");

        private static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
                .column("name", createUnboundedVarcharType())
                .column("value", createUnboundedVarcharType())
                .build();

        @Override
        public Distribution getDistribution()
        {
            return ALL_NODES;
        }

        @Override
        public ConnectorTableMetadata getTableMetadata()
        {
            return METADATA;
        }

        @Override
        public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
        {
            InMemoryRecordSet.Builder table = InMemoryRecordSet.builder(METADATA);
            session.getIdentity().getExtraCredentials().forEach(table::addRow);
            return table.build().cursor();
        }
    }
}

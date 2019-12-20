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
package io.prestosql.jdbc;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.log.Logging;
import io.prestosql.plugin.hive.HiveHadoop2Plugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.predicate.TupleDomain;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Set;

import static com.google.inject.multibindings.Multibinder.newSetBinder;
import static io.prestosql.jdbc.TestPrestoDriver.closeQuietly;
import static io.prestosql.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static io.prestosql.spi.connector.SystemTable.Distribution.ALL_NODES;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestJdbcConnection
{
    private TestingPrestoServer server;

    @BeforeClass
    public void setupServer()
            throws Exception
    {
        Logging.initialize();
        Module systemTables = binder -> newSetBinder(binder, SystemTable.class)
                .addBinding().to(ExtraCredentialsSystemTable.class).in(Scopes.SINGLETON);
        server = new TestingPrestoServer(ImmutableList.of(systemTables));
        server.installPlugin(new HiveHadoop2Plugin());
        server.createCatalog("hive", "hive-hadoop2", ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", server.getBaseDataDir().resolve("hive").toAbsolutePath().toString())
                .put("hive.security", "sql-standard")
                .build());

        try (Connection connection = createConnection();
                Statement statement = connection.createStatement()) {
            statement.execute("SET ROLE admin");
            statement.execute("CREATE SCHEMA default");
            statement.execute("CREATE SCHEMA fruit");
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDownServer()
    {
        closeQuietly(server);
    }

    @Test
    public void testCommit()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_commit (x bigint)");
            }

            try (Connection otherConnection = createConnection()) {
                assertThat(listTables(otherConnection)).doesNotContain("test_commit");
            }

            connection.commit();
        }

        try (Connection connection = createConnection()) {
            assertThat(listTables(connection)).contains("test_commit");
        }
    }

    @Test
    public void testRollback()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE TABLE test_rollback (x bigint)");
            }

            try (Connection otherConnection = createConnection()) {
                assertThat(listTables(otherConnection)).doesNotContain("test_rollback");
            }

            connection.rollback();
        }

        try (Connection connection = createConnection()) {
            assertThat(listTables(connection)).doesNotContain("test_rollback");
        }
    }

    @Test
    public void testUse()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertThat(connection.getCatalog()).isEqualTo("hive");
            assertThat(connection.getSchema()).isEqualTo("default");

            // change schema
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE fruit");
            }

            assertThat(connection.getCatalog()).isEqualTo("hive");
            assertThat(connection.getSchema()).isEqualTo("fruit");

            // change catalog and schema
            try (Statement statement = connection.createStatement()) {
                statement.execute("USE system.runtime");
            }

            assertThat(connection.getCatalog()).isEqualTo("system");
            assertThat(connection.getSchema()).isEqualTo("runtime");

            // run multiple queries
            assertThat(listTables(connection)).contains("nodes");
            assertThat(listTables(connection)).contains("queries");
            assertThat(listTables(connection)).contains("tasks");
        }
    }

    @Test
    public void testSession()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertThat(listSession(connection))
                    .contains("join_distribution_type|PARTITIONED|PARTITIONED")
                    .contains("exchange_compression|false|false");

            try (Statement statement = connection.createStatement()) {
                statement.execute("SET SESSION join_distribution_type = 'BROADCAST'");
            }

            assertThat(listSession(connection))
                    .contains("join_distribution_type|BROADCAST|PARTITIONED")
                    .contains("exchange_compression|false|false");

            try (Statement statement = connection.createStatement()) {
                statement.execute("SET SESSION exchange_compression = true");
            }

            assertThat(listSession(connection))
                    .contains("join_distribution_type|BROADCAST|PARTITIONED")
                    .contains("exchange_compression|true|false");

            try (Statement statement = connection.createStatement()) {
                // setting Hive session properties requires the admin role
                statement.execute("SET ROLE admin");
            }

            for (String part : ImmutableList.of(",", "=", ":", "|", "/", "\\", "'", "\\'", "''", "\"", "\\\"", "[", "]")) {
                String value = format("/tmp/presto-%s-${USER}", part);
                try {
                    try (Statement statement = connection.createStatement()) {
                        statement.execute(format("SET SESSION hive.temporary_staging_directory_path = '%s'", value.replace("'", "''")));
                    }

                    assertThat(listSession(connection))
                            .contains("join_distribution_type|BROADCAST|PARTITIONED")
                            .contains("exchange_compression|true|false")
                            .contains(format("hive.temporary_staging_directory_path|%s|/tmp/presto-${USER}", value));
                }
                catch (Exception e) {
                    fail(format("Failed to set session property value to [%s]", value), e);
                }
            }
        }
    }

    @Test
    public void testApplicationName()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            assertConnectionSource(connection, "presto-jdbc");
        }

        try (Connection connection = createConnection()) {
            connection.setClientInfo("ApplicationName", "testing");
            assertConnectionSource(connection, "testing");
        }

        try (Connection connection = createConnection("applicationNamePrefix=fruit:")) {
            assertConnectionSource(connection, "fruit:");
        }

        try (Connection connection = createConnection("applicationNamePrefix=fruit:")) {
            connection.setClientInfo("ApplicationName", "testing");
            assertConnectionSource(connection, "fruit:testing");
        }
    }

    @Test
    public void testExtraCredentials()
            throws SQLException
    {
        Map<String, String> credentials = ImmutableMap.of("test.token.foo", "bar", "test.token.abc", "xyz");
        Connection connection = createConnection("extraCredentials=test.token.foo:bar;test.token.abc:xyz");
        assertTrue(connection instanceof PrestoConnection);
        PrestoConnection prestoConnection = connection.unwrap(PrestoConnection.class);
        assertEquals(prestoConnection.getExtraCredentials(), credentials);
        assertEquals(listExtraCredentials(connection), credentials);
    }

    private Connection createConnection()
            throws SQLException
    {
        return createConnection("");
    }

    private Connection createConnection(String extra)
            throws SQLException
    {
        String url = format("jdbc:presto://%s/hive/default?%s", server.getAddress(), extra);
        return DriverManager.getConnection(url, "admin", null);
    }

    private static Set<String> listTables(Connection connection)
            throws SQLException
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
                set.add(rs.getString(1));
            }
        }
        return set.build();
    }

    private static Set<String> listSession(Connection connection)
            throws SQLException
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SHOW SESSION")) {
            while (rs.next()) {
                set.add(Joiner.on('|').join(
                        rs.getString(1),
                        rs.getString(2),
                        rs.getString(3)));
            }
        }
        return set.build();
    }

    private static Map<String, String> listExtraCredentials(Connection connection)
            throws SQLException
    {
        ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM system.test.extra_credentials");
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        while (rs.next()) {
            builder.put(rs.getString("name"), rs.getString("value"));
        }
        return builder.build();
    }

    private static void assertConnectionSource(Connection connection, String expectedSource)
            throws SQLException
    {
        String queryId;
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery("SELECT 123")) {
            queryId = rs.unwrap(PrestoResultSet.class).getQueryId();
        }

        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT source FROM system.runtime.queries WHERE query_id = ?")) {
            statement.setString(1, queryId);
            try (ResultSet rs = statement.executeQuery()) {
                assertTrue(rs.next());
                assertThat(rs.getString("source")).isEqualTo(expectedSource);
                assertFalse(rs.next());
            }
        }
    }

    private static class ExtraCredentialsSystemTable
            implements SystemTable
    {
        private static final SchemaTableName NAME = new SchemaTableName("test", "extra_credentials");

        public static final ConnectorTableMetadata METADATA = tableMetadataBuilder(NAME)
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

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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code ON CLUSTER} clause the connector adds to DDL when
 * {@code clickhouse.cluster-name} is set.
 * <p>
 * ClickHouse only accepts the clause in two places: directly after the table name for
 * {@code CREATE TABLE} and {@code ALTER TABLE}, and at the end of the statement for
 * {@code CREATE}, {@code DROP} and {@code RENAME DATABASE}, {@code RENAME TABLE} and
 * {@code DROP TABLE}. Anywhere else it is a syntax error, so these tests assert the exact
 * statement text ClickHouse received rather than merely that the statement succeeded.
 */
public class TestClickHouseClusterDdl
        extends AbstractTestQueryFramework
{
    private static final String CLUSTER_NAME = "test_cluster";
    private static final String NO_CLUSTER_CATALOG = "clickhouse_no_cluster";
    private static final String BLANK_CLUSTER_CATALOG = "clickhouse_blank_cluster";

    private TestingClickHouseServer clickhouseServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE, true));
        DistributedQueryRunner queryRunner = ClickHouseQueryRunner.builder(clickhouseServer)
                .addConnectorProperty("clickhouse.cluster-name", CLUSTER_NAME)
                .addConnectorProperty("clickhouse.map-string-as-varchar", "true")
                .build();
        // Two more catalogs over the same server, to assert the connector stays silent about
        // clusters unless clickhouse.cluster-name actually names one.
        queryRunner.createCatalog(NO_CLUSTER_CATALOG, "clickhouse", ImmutableMap.of(
                "connection-url", clickhouseServer.getJdbcUrl(),
                "connection-user", clickhouseServer.getUsername(),
                "connection-password", clickhouseServer.getPassword(),
                "clickhouse.map-string-as-varchar", "true"));
        queryRunner.createCatalog(BLANK_CLUSTER_CATALOG, "clickhouse", ImmutableMap.of(
                "connection-url", clickhouseServer.getJdbcUrl(),
                "connection-user", clickhouseServer.getUsername(),
                "connection-password", clickhouseServer.getPassword(),
                "clickhouse.map-string-as-varchar", "true",
                "clickhouse.cluster-name", ""));
        return queryRunner;
    }

    @Test
    public void testSchemaDdl()
    {
        String schemaName = "test_cluster_schema_" + randomNameSuffix();
        String renamedSchemaName = schemaName + "_renamed";
        try {
            assertUpdate("CREATE SCHEMA " + schemaName);
            assertClusteredDdl(schemaName, "CREATE DATABASE .* ON CLUSTER " + CLUSTER_NAME);

            assertUpdate("ALTER SCHEMA " + schemaName + " RENAME TO " + renamedSchemaName);
            assertClusteredDdl(renamedSchemaName, "RENAME DATABASE .* ON CLUSTER " + CLUSTER_NAME);

            assertUpdate("DROP SCHEMA " + renamedSchemaName);
            assertClusteredDdl(renamedSchemaName, "DROP DATABASE .* ON CLUSTER " + CLUSTER_NAME);
        }
        finally {
            assertUpdate("DROP SCHEMA IF EXISTS " + schemaName);
            assertUpdate("DROP SCHEMA IF EXISTS " + renamedSchemaName);
        }
    }

    @Test
    public void testCreateTable()
    {
        String tableName = "test_cluster_create_" + randomNameSuffix();
        try {
            assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL, name varchar) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
            // the clause has to sit between the table name and the column list
            assertClusteredDdl(tableName, "CREATE TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(.*");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testDropTable()
    {
        String tableName = "test_cluster_drop_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");

        assertUpdate("DROP TABLE " + tableName);
        // the clause has to come at the end for DROP TABLE
        assertClusteredDdl(tableName, "DROP TABLE .* ON CLUSTER " + CLUSTER_NAME);
    }

    @Test
    public void testRenameTable()
    {
        String tableName = "test_cluster_rename_" + randomNameSuffix();
        String renamedTableName = tableName + "_renamed";
        assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            assertUpdate("ALTER TABLE " + tableName + " RENAME TO " + renamedTableName);
            // the clause has to come at the end for RENAME TABLE, after the target name
            assertClusteredDdl(renamedTableName, "RENAME TABLE .* ON CLUSTER " + CLUSTER_NAME);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + renamedTableName);
        }
    }

    @Test
    public void testColumnDdl()
    {
        String tableName = "test_cluster_column_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            assertUpdate("ALTER TABLE " + tableName + " ADD COLUMN name varchar");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(ADD COLUMN .*");

            assertUpdate("ALTER TABLE " + tableName + " RENAME COLUMN name TO renamed");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(RENAME COLUMN .*");

            assertUpdate("ALTER TABLE " + tableName + " DROP COLUMN renamed");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(DROP COLUMN .*");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testCommentDdl()
    {
        String tableName = "test_cluster_comment_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            assertUpdate("COMMENT ON TABLE " + tableName + " IS 'table comment'");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(MODIFY COMMENT .*");

            assertUpdate("COMMENT ON COLUMN " + tableName + ".id IS 'column comment'");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(COMMENT COLUMN .*");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testSetTableProperties()
    {
        String tableName = "test_cluster_properties_" + randomNameSuffix();
        // ClickHouse requires the sampling key to be an unsigned integer already present in the sorting key
        assertUpdate("CREATE TABLE " + tableName + " (p1 int NOT NULL, p2 boolean NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['p1', 'p2'])");
        try {
            assertUpdate("ALTER TABLE " + tableName + " SET PROPERTIES sample_by = 'p2'");
            assertClusteredDdl(tableName, "ALTER TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(MODIFY SAMPLE BY .*");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testInsertStagesTemporaryTableOnCluster()
    {
        String tableName = "test_cluster_insert_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (id bigint NOT NULL, name varchar) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
        try {
            // Insert stages rows in a temporary table, and that temporary table has to exist on the
            // cluster too, otherwise the create and the insert can land on different nodes.
            List<String> before = ddlQueueQueries("");
            assertUpdate("INSERT INTO " + tableName + " VALUES (1, 'a')", 1);
            List<String> staged = new ArrayList<>(ddlQueueQueries(""));
            staged.removeAll(before);

            assertThat(staged)
                    .as("Statements issued on the cluster while staging an insert")
                    .anyMatch(query -> query.matches("CREATE TABLE .* ON CLUSTER " + CLUSTER_NAME + " \\(.*"))
                    .anyMatch(query -> query.matches("DROP TABLE .* ON CLUSTER " + CLUSTER_NAME));
            assertQuery("SELECT id, name FROM " + tableName, "VALUES (1, 'a')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + tableName);
        }
    }

    @Test
    public void testClusterNameIsNotUsedWhenNotConfigured()
    {
        assertNoClusterDdl(Session.builder(getSession()).setCatalog(NO_CLUSTER_CATALOG).build());
        // A blank cluster name has to behave like an unset one, rather than being spliced into the
        // DDL as `ON CLUSTER ""`, which every statement rejects.
        assertNoClusterDdl(Session.builder(getSession()).setCatalog(BLANK_CLUSTER_CATALOG).build());
    }

    private void assertNoClusterDdl(Session session)
    {
        String tableName = "test_no_cluster_" + randomNameSuffix();
        try {
            assertUpdate(session, "CREATE TABLE " + tableName + " (id bigint NOT NULL) WITH (engine = 'MergeTree', order_by = ARRAY['id'])");
            assertUpdate(session, "ALTER TABLE " + tableName + " ADD COLUMN name varchar");

            assertThat(ddlQueueQueries(""))
                    .as("DDL must not be routed to a cluster unless clickhouse.cluster-name names one")
                    .noneMatch(query -> query.contains(tableName));
        }
        finally {
            assertUpdate(session, "DROP TABLE IF EXISTS " + tableName);
        }
    }

    private void assertClusteredDdl(String tableName, String expectedQuery)
    {
        assertThat(ddlQueueQueries(tableName))
                .as("DDL executed on cluster %s for %s", CLUSTER_NAME, tableName)
                .anyMatch(query -> query.matches(expectedQuery));
    }

    /**
     * Returns the statements ClickHouse recorded in its distributed DDL queue, which is where
     * {@code ON CLUSTER} statements are logged. Only statements carrying the clause appear here,
     * so this shows both that the clause was sent and exactly how it was spelled.
     *
     * @param nameFilter only return statements mentioning this string, or the empty string for all
     */
    private List<String> ddlQueueQueries(String nameFilter)
    {
        try (Connection connection = DriverManager.getConnection(
                clickhouseServer.getJdbcUrl(),
                clickhouseServer.getUsername(),
                clickhouseServer.getPassword());
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT query FROM system.distributed_ddl_queue WHERE cluster = ? AND query LIKE ?")) {
            statement.setString(1, CLUSTER_NAME);
            statement.setString(2, "%" + nameFilter + "%");
            try (ResultSet resultSet = statement.executeQuery()) {
                List<String> queries = new ArrayList<>();
                while (resultSet.next()) {
                    queries.add(resultSet.getString(1));
                }
                return queries;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

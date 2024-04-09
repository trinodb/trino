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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.connector.Grants;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MutableGrants;
import io.trino.spi.connector.CatalogSchemaTableName;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.connector.ConnectorViewDefinition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.QueryAssertions.assertUpdate;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestGrantOnTable
{
    private final SchemaTableName table = new SchemaTableName("default", "table_one");
    private final SchemaTableName view = new SchemaTableName("default", "test_view");
    private final SchemaTableName materializedView = new SchemaTableName("default", "test_materialized_view");
    private final Session admin = sessionOf("admin");
    private final Grants<SchemaTableName> tableGrants = new MutableGrants<>();
    private QueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeAll
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(admin).build();
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("default"))
                .withListTables((session, schemaName) -> "default".equalsIgnoreCase(schemaName)
                        ? ImmutableList.of(table.getTableName(), view.getTableName(), materializedView.getTableName())
                        : ImmutableList.of())
                .withGetTableHandle((session, tableName) -> tableName.equals(table) ? new MockConnectorTableHandle(tableName) : null)
                .withSchemaGrants(new MutableGrants<>())
                .withTableGrants(tableGrants)
                .withGetViews((session, schemaTablePrefix) ->
                        ImmutableMap.of(
                                view, new ConnectorViewDefinition(
                                        "SELECT nationkey AS test_column FROM tpch.tiny.nation",
                                        Optional.empty(),
                                        Optional.empty(),
                                        ImmutableList.of(new ConnectorViewDefinition.ViewColumn("test_column", BIGINT.getTypeId(), Optional.empty())),
                                        Optional.empty(),
                                        Optional.empty(),
                                        true,
                                        ImmutableList.of())))
                .withGetMaterializedViews((connectorSession, prefix) ->
                        ImmutableMap.of(
                                materializedView, new ConnectorMaterializedViewDefinition(
                                        "SELECT nationkey AS test_column FROM tpch.tiny.nation",
                                        Optional.of(new CatalogSchemaTableName("mock", "default", "test_materialized_view$materialized_view_storage")),
                                        Optional.of("mock"),
                                        Optional.of("default"),
                                        ImmutableList.of(new ConnectorMaterializedViewDefinition.Column("test_column", BIGINT.getTypeId(), Optional.empty())),
                                        Optional.of(Duration.ZERO),
                                        Optional.empty(),
                                        Optional.of("alice"),
                                        ImmutableList.of())))
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), table, EnumSet.allOf(Privilege.class), true);
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), view, EnumSet.allOf(Privilege.class), true);
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), materializedView, EnumSet.allOf(Privilege.class), true);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
        queryRunner = null; // closed by assertions.close
    }

    @Test
    public void testExistingGrants()
    {
        testExistingGrants(true);
        testExistingGrants(false);
    }

    private void testExistingGrants(boolean grantOption)
    {
        String username = randomUsername();
        Session user = sessionOf(username);
        tableGrants.grant(new TrinoPrincipal(USER, username), table, EnumSet.allOf(Privilege.class), grantOption);

        assertThat(assertions.query(admin, "SHOW TABLES FROM local.default"))
                .matches("VALUES VARCHAR 'table_one', 'test_view', 'test_materialized_view'");
        assertThat(assertions.query(user, "SHOW TABLES FROM local.default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test
    public void testValidGrant()
    {
        testValidGrant("CREATE");
        testValidGrant("SELECT");
        testValidGrant("INSERT");
        testValidGrant("UPDATE");
        testValidGrant("DELETE");
        testValidGrant("ALL PRIVILEGES");
    }

    private void testValidGrant(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON TABLE table_one TO %s", privilege, username));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test
    public void testGrantOnView()
    {
        String username = randomUsername();
        Session user = sessionOf(username);
        assertThat(assertions.query(user, "SHOW TABLES FROM default"))
                .result().isEmpty();
        queryRunner.execute(admin, format("GRANT SELECT ON TABLE test_view TO %s", username));
        assertThat(assertions.query(user, "SHOW TABLES FROM default"))
                .matches("VALUES (VARCHAR 'test_view')");
    }

    @Test
    public void testGrantOnMaterializedView()
    {
        String username = randomUsername();
        Session user = sessionOf(username);
        assertThat(assertions.query(user, "SHOW TABLES FROM default"))
                .result().isEmpty();
        queryRunner.execute(admin, format("GRANT SELECT ON TABLE test_materialized_view TO %s", username));
        assertThat(assertions.query(user, "SHOW TABLES FROM default"))
                .matches("VALUES (VARCHAR 'test_materialized_view')");
    }

    @Test
    public void testValidGrantWithGrantOption()
    {
        testValidGrantWithGrantOption("CREATE");
        testValidGrantWithGrantOption("SELECT");
        testValidGrantWithGrantOption("INSERT");
        testValidGrantWithGrantOption("UPDATE");
        testValidGrantWithGrantOption("DELETE");
        testValidGrantWithGrantOption("ALL PRIVILEGES");
    }

    private void testValidGrantWithGrantOption(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON TABLE table_one TO %s WITH GRANT OPTION", privilege, username));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertUpdate(queryRunner, user, format("GRANT %s ON TABLE table_one TO %s", privilege, randomUsername()), OptionalLong.empty(), Optional.empty());
        assertUpdate(queryRunner, user, format("GRANT %s ON TABLE table_one TO %s WITH GRANT OPTION", privilege, randomUsername()), OptionalLong.empty(), Optional.empty());
    }

    @Test
    public void testGrantOnNonExistingCatalog()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT CREATE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT SELECT ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT INSERT ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT UPDATE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT DELETE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT ALL PRIVILEGES ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testGrantOnNonExistingSchema()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT CREATE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT SELECT ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT INSERT ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT UPDATE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT DELETE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT ALL PRIVILEGES ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testGrantOnNonExistingTable()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT CREATE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT SELECT ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT INSERT ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT UPDATE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT DELETE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT ALL PRIVILEGES ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
    }

    private static Session sessionOf(String username)
    {
        return testSessionBuilder()
                .setIdentity(Identity.ofUser(username))
                .setCatalog("local")
                .setSchema("default")
                .build();
    }
}

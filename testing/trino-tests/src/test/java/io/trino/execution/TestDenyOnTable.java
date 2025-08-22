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
import com.google.common.collect.ImmutableSet;
import io.trino.Session;
import io.trino.connector.Grants;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.connector.MutableGrants;
import io.trino.metadata.DisabledSystemSecurityMetadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.SystemSecurityMetadata;
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

import java.util.EnumSet;
import java.util.Set;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.security.Privilege.CREATE_BRANCH;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ROOT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_CLASS)
@Execution(SAME_THREAD)
public class TestDenyOnTable
{
    private final SchemaTableName table = new SchemaTableName("default", "table_one");
    private final Session admin = sessionOf("admin");
    private final Grants<SchemaTableName> tableGrants = new MutableGrants<>();
    private QueryRunner queryRunner;
    private QueryAssertions assertions;

    private QualifiedObjectName expectedTableName;
    private Set<Privilege> expectedPrivileges;
    private TrinoPrincipal expectedGrantee;
    private boolean denyCalled;

    @BeforeAll
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(admin)
                .setAdditionalModule(binder -> {
                    newOptionalBinder(binder, SystemSecurityMetadata.class)
                            .setBinding()
                            .toInstance(new DisabledSystemSecurityMetadata()
                            {
                                @Override
                                public void denyTablePrivileges(Session session, QualifiedObjectName tableName, Set<Privilege> privileges, TrinoPrincipal grantee)
                                {
                                    assertThat(expectedTableName).isEqualTo(tableName);
                                    assertThat(expectedPrivileges).isEqualTo(privileges);
                                    assertThat(expectedGrantee).isEqualTo(grantee);
                                    assertThat(denyCalled).isFalse();
                                    denyCalled = true;
                                }
                            });
                })
                .build();
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("default"))
                .withListTables((session, schemaName) -> "default".equalsIgnoreCase(schemaName) ? ImmutableList.of(table.getTableName()) : ImmutableList.of())
                .withGetTableHandle((session, tableName) -> tableName.equals(table) ? new MockConnectorTableHandle(tableName) : null)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), table, EnumSet.allOf(Privilege.class), true);
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
        queryRunner = null; // closed by assertions.close
    }

    @Test
    public void testValidDenyTable()
    {
        testValidDenyTable("CREATE");
        testValidDenyTable("SELECT");
        testValidDenyTable("INSERT");
        testValidDenyTable("UPDATE");
        testValidDenyTable("DELETE");
        testValidDenyTable("CREATE BRANCH");
        testValidDenyTable("ALL PRIVILEGES");
    }

    private void testValidDenyTable(String privilege)
    {
        String username = randomUsername();

        denyCalled = false;
        expectedTableName = new QualifiedObjectName("local", "default", "table_one");
        if (privilege.equalsIgnoreCase("all privileges")) {
            expectedPrivileges = ImmutableSet.copyOf(Privilege.values());
        }
        else if (privilege.equalsIgnoreCase("create branch")) {
            expectedPrivileges = ImmutableSet.of(CREATE_BRANCH);
        }
        else {
            expectedPrivileges = ImmutableSet.of(Privilege.valueOf(privilege.toUpperCase(ROOT)));
        }
        expectedGrantee = new TrinoPrincipal(USER, username);

        queryRunner.execute(admin, format("DENY %s ON TABLE table_one TO %s", privilege, username));
        assertThat(denyCalled).isTrue();
    }

    @Test
    public void testDenyOnNonExistingCatalog()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY CREATE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY SELECT ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY INSERT ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY UPDATE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY DELETE ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY ALL PRIVILEGES ON TABLE missing_catalog.missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testDenyOnNonExistingSchema()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY CREATE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY SELECT ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY INSERT ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY UPDATE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY DELETE ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY ALL PRIVILEGES ON TABLE missing_schema.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
    }

    @Test
    public void testDenyOnNonExistingTable()
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY CREATE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY SELECT ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY INSERT ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY UPDATE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY DELETE ON TABLE default.missing_table TO %s", randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
        assertThatThrownBy(() -> queryRunner.execute(admin, format("DENY ALL PRIVILEGES ON TABLE default.missing_table TO %s", randomUsername())))
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

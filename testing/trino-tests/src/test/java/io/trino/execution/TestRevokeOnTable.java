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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.Identity;
import io.trino.spi.security.Privilege;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.EnumSet;

import static io.trino.common.Randoms.randomUsername;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRevokeOnTable
{
    private static final Session admin = sessionOf("admin");
    private static final Session userWithAllPrivileges = sessionOf(randomUsername());
    private static final Session userWithSelect = sessionOf(randomUsername());
    private static final Session userWithInsert = sessionOf(randomUsername());
    private static final Session userWithUpdate = sessionOf(randomUsername());
    private static final Session userWithDelete = sessionOf(randomUsername());
    private DistributedQueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeClass
    public void initClass()
            throws Exception
    {
        SchemaTableName table = new SchemaTableName("default", "table_one");
        queryRunner = DistributedQueryRunner.builder(userWithAllPrivileges).build();
        Grants<SchemaTableName> tableGrants = new MutableGrants<>();
        tableGrants.grant(new TrinoPrincipal(USER, admin.getUser()), table, EnumSet.allOf(Privilege.class), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithAllPrivileges.getUser()), table, EnumSet.allOf(Privilege.class), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithSelect.getUser()), table, ImmutableSet.of(Privilege.SELECT), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithInsert.getUser()), table, ImmutableSet.of(Privilege.INSERT), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithUpdate.getUser()), table, ImmutableSet.of(Privilege.UPDATE), true);
        tableGrants.grant(new TrinoPrincipal(USER, userWithDelete.getUser()), table, ImmutableSet.of(Privilege.DELETE), true);
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("default"))
                .withListTables((session, schemaName) -> "default".equalsIgnoreCase(schemaName) ? ImmutableList.of(table) : ImmutableList.of())
                .withGetTableHandle((session, tableName) -> tableName.equals(table) ? new MockConnectorTableHandle(tableName) : null)
                .withSchemaGrants(new MutableGrants<>())
                .withTableGrants(tableGrants)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnSchema(String privilege, Session user)
    {
        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");

        queryRunner.execute(admin, format("REVOKE %s ON TABLE table_one FROM %s", privilege, user.getUser()));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).returnsEmptyResult();
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnNonExistingCatalog(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE missing_catalog.missing_schema.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnNonExistingSchema(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE missing_schema.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
    }

    @Test(dataProvider = "privilegesAndUsers")
    public void testRevokeOnNonExistingTable(String privilege, Session user)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("REVOKE %s ON TABLE default.missing_table FROM %s", privilege, user.getUser())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testAccessDenied(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("REVOKE %s ON SCHEMA default FROM %s", privilege, randomUsername())))
                .hasMessageContaining(format(
                        "Access Denied: Cannot revoke privilege %s on schema default",
                        privilege.equals("ALL PRIVILEGES") ? "SELECT" : privilege));
    }

    @DataProvider(name = "privilegesAndUsers")
    public static Object[][] privilegesAndUsers()
    {
        return new Object[][] {
                {"SELECT", userWithSelect},
                {"INSERT", userWithInsert},
                {"UPDATE", userWithUpdate},
                {"DELETE", userWithDelete},
                {"ALL PRIVILEGES", userWithAllPrivileges}
        };
    }

    @DataProvider(name = "privileges")
    public static Object[][] privileges()
    {
        return new Object[][] {
                {"SELECT"},
                {"INSERT"},
                {"UPDATE"},
                {"DELETE"},
                {"ALL PRIVILEGES"}
        };
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

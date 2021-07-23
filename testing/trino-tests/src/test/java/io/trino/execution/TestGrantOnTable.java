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
import io.trino.testing.DataProviders;
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

public class TestGrantOnTable
{
    private final SchemaTableName table = new SchemaTableName("default", "table_one");
    private final Session admin = sessionOf("admin");
    private final Grants<SchemaTableName> tableGrants = new MutableGrants<>();
    private DistributedQueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeClass
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(admin).build();
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
        tableGrants.grant(new TrinoPrincipal(USER, "admin"), table, EnumSet.allOf(Privilege.class), true);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test(dataProviderClass = DataProviders.class, dataProvider = "trueFalse")
    public void testExistingGrants(boolean grantOption)
    {
        String username = randomUsername();
        Session user = sessionOf(username);
        tableGrants.grant(new TrinoPrincipal(USER, username), table, EnumSet.allOf(Privilege.class), grantOption);

        assertThat(assertions.query(admin, "SHOW TABLES FROM local.default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(assertions.query(user, "SHOW TABLES FROM local.default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test(dataProvider = "privileges")
    public void testValidGrant(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON TABLE table_one TO %s", privilege, username));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test(dataProvider = "privileges")
    public void testValidGrantWithGrantOption(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON TABLE table_one TO %s WITH GRANT OPTION", privilege, username));

        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(assertions.query(user, format("GRANT %s ON TABLE table_one TO %s", privilege, randomUsername()))).matches("VALUES (BOOLEAN 'TRUE')");
        assertThat(assertions.query(user, format("GRANT %s ON TABLE table_one TO %s WITH GRANT OPTION", privilege, randomUsername()))).matches("VALUES (BOOLEAN 'TRUE')");
    }

    @Test(dataProvider = "privileges")
    public void testGrantOnNonExistingCatalog(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT %s ON TABLE missing_catalog.missing_schema.missing_table TO %s", privilege, randomUsername())))
                .hasMessageContaining("Table 'missing_catalog.missing_schema.missing_table' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testGrantOnNonExistingSchema(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT %s ON TABLE missing_schema.missing_table TO %s", privilege, randomUsername())))
                .hasMessageContaining("Table 'local.missing_schema.missing_table' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testGrantOnNonExistingTable(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT %s ON TABLE default.missing_table TO %s", privilege, randomUsername())))
                .hasMessageContaining("Table 'local.default.missing_table' does not exist");
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

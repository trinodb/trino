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
package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.connector.Grants;
import io.prestosql.connector.MockConnectorFactory;
import io.prestosql.connector.MockConnectorPlugin;
import io.prestosql.connector.MutableGrants;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.sql.query.QueryAssertions;
import io.prestosql.testing.DataProviders;
import io.prestosql.testing.DistributedQueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.EnumSet;

import static io.prestosql.common.Randoms.randomUsername;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGrantOnSchema
{
    private final Session admin = sessionOf("admin");
    private final Grants<String> schemaGrants = new MutableGrants<>();
    private DistributedQueryRunner queryRunner;
    private QueryAssertions assertions;

    @BeforeClass
    public void initClass()
            throws Exception
    {
        queryRunner = DistributedQueryRunner.builder(admin).build();
        MockConnectorFactory connectorFactory = MockConnectorFactory.builder()
                .withListSchemaNames(session -> ImmutableList.of("information_schema", "default"))
                .withListTables((session, schema) ->
                        "default".equalsIgnoreCase(schema) ? ImmutableList.of(new SchemaTableName(schema, "table_one")) : ImmutableList.of())
                .withSchemaGrants(schemaGrants)
                .build();
        queryRunner.installPlugin(new MockConnectorPlugin(connectorFactory));
        queryRunner.createCatalog("local", "mock");
        assertions = new QueryAssertions(queryRunner);
        schemaGrants.grant(new PrestoPrincipal(USER, admin.getUser()), "default", EnumSet.allOf(Privilege.class), true);
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
        schemaGrants.grant(new PrestoPrincipal(USER, user.getUser()), "default", EnumSet.allOf(Privilege.class), grantOption);

        assertThat(assertions.query(admin, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(assertions.query(admin, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test(dataProvider = "privileges")
    public void testValidGrant(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON SCHEMA default TO %s", privilege, username));

        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(assertions.query(admin, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
        assertThat(assertions.query(user, "SHOW TABLES FROM default")).matches("VALUES (VARCHAR 'table_one')");
    }

    @Test(dataProvider = "privileges")
    public void testValidGrantWithGrantOption(String privilege)
    {
        String username = randomUsername();
        Session user = sessionOf(username);

        queryRunner.execute(admin, format("GRANT %s ON SCHEMA default TO %s WITH GRANT OPTION", privilege, username));

        assertThat(assertions.query(user, "SHOW SCHEMAS FROM local")).matches("VALUES (VARCHAR 'information_schema'), (VARCHAR 'default')");
        assertThat(assertions.query(user, format("GRANT %s ON SCHEMA default TO %s", privilege, randomUsername()))).matches("VALUES (BOOLEAN 'TRUE')");
        assertThat(assertions.query(user, format("GRANT %s ON SCHEMA default TO %s WITH GRANT OPTION", privilege, randomUsername()))).matches("VALUES (BOOLEAN 'TRUE')");
    }

    @Test(dataProvider = "privileges")
    public void testGrantOnNonExistingCatalog(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT %s ON SCHEMA missing_catalog.missing_schema TO %s", privilege, randomUsername())))
                .hasMessageContaining("Schema 'missing_catalog.missing_schema' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testGrantOnNonExistingSchema(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(admin, format("GRANT %s ON SCHEMA missing_schema TO %s", privilege, randomUsername())))
                .hasMessageContaining("Schema 'local.missing_schema' does not exist");
    }

    @Test(dataProvider = "privileges")
    public void testAccessDenied(String privilege)
    {
        assertThatThrownBy(() -> queryRunner.execute(sessionOf(randomUsername()), format("GRANT %s ON SCHEMA default TO %s", privilege, randomUsername())))
                .hasMessageContaining("Access Denied: Cannot grant privilege SELECT on schema default");
    }

    @DataProvider(name = "privileges")
    public static Object[][] privileges()
    {
        return new Object[][] {
                {"SELECT"},
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
